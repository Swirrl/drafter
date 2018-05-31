(ns drafter.stasher
  (:require [clojure.java.io :as io]
            [clojure.spec.alpha :as s]
            [drafter.stasher.filecache :as fc]
            [drafter.stasher.cache-key :as ck]
            [drafter.stasher.timing :as timing]
            [grafter.rdf4j.repository :as repo]
            [grafter.rdf4j.sparql :as sparql]
            [grafter.rdf4j.io :as gio]
            [integrant.core :as ig]
            [clojure.tools.logging :as log]
            [cognician.dogstatsd :as dd]
            [me.raynes.fs :as fs]
            [drafter.backend.common :as drpr]
            [grafter.rdf4j.repository.registry :as reg])
  (:import java.net.URI
           (drafter.rdf DrafterSPARQLConnection DrafterSPARQLRepository)
           java.nio.charset.Charset
           org.eclipse.rdf4j.query.impl.BackgroundGraphResult
           (org.eclipse.rdf4j.query Dataset GraphQueryResult QueryLanguage
                                    TupleQueryResultHandler TupleQueryResult)
           org.eclipse.rdf4j.query.resultio.helpers.BackgroundTupleResult
           (org.eclipse.rdf4j.query.resultio TupleQueryResultFormat BooleanQueryResultFormat QueryResultIO
                                             TupleQueryResultWriter BooleanQueryResultParserRegistry
                                             TupleQueryResultParserRegistry)
           org.eclipse.rdf4j.repository.Repository
           org.eclipse.rdf4j.repository.RepositoryConnection
           (org.eclipse.rdf4j.repository.sparql.query SPARQLBooleanQuery SPARQLGraphQuery SPARQLTupleQuery SPARQLUpdate)
           (org.eclipse.rdf4j.rio RDFFormat RDFHandler RDFWriter RDFParserRegistry)
           (java.util.concurrent ThreadPoolExecutor TimeUnit ArrayBlockingQueue)
           [org.eclipse.rdf4j.query.resultio.sparqljson SPARQLBooleanJSONParserFactory SPARQLResultsJSONParserFactory]
           [org.eclipse.rdf4j.query.resultio.sparqlxml SPARQLBooleanXMLParserFactory SPARQLResultsXMLParserFactory]
           [org.eclipse.rdf4j.query.resultio.binary BinaryQueryResultParserFactory]
           org.eclipse.rdf4j.query.resultio.text.BooleanTextParserFactory
           org.eclipse.rdf4j.rio.nquads.NQuadsParserFactory
           org.eclipse.rdf4j.rio.ntriples.NTriplesParserFactory
           org.eclipse.rdf4j.rio.turtle.TurtleParserFactory
           org.eclipse.rdf4j.rio.trig.TriGParserFactory
           org.eclipse.rdf4j.rio.binary.BinaryRDFParserFactory))

(extend-type Repository
  drpr/SparqlExecutor
  (drpr/prepare-query [this sparql-string]
    (drpr/prep-and-validate-query this sparql-string))

  drpr/ToRepository
  (drpr/->sesame-repo [r] r))

(s/def ::core-pool-size pos-int?)
(s/def ::max-pool-size pos-int?)
(s/def ::keep-alive-time-ms integer?)
(s/def ::queue-size pos-int?)

(defmethod ig/pre-init-spec ::cache-thread-pool [_]
  (s/keys :opt-un [::core-pool-size ::max-pool-size ::keep-alive-time-ms ::queue-size]))

;; This threadpool will raise a java.util.concurrent.RejectedExecutionException
;; when there are no free threads to execute tasks and the queue of
;; waiting jobs is full.
(defmethod ig/init-key ::cache-thread-pool [_ opts]
  (let [{:keys [core-pool-size
                max-pool-size
                keep-alive-time-ms ;; time threads above core-pool-size wait for work before dying.
                queue-size]} opts
        queue (ArrayBlockingQueue. (or queue-size 1))]
    (ThreadPoolExecutor. (or core-pool-size 1)
                         (or max-pool-size 1)
                         (or keep-alive-time-ms 1000)
                         TimeUnit/MILLISECONDS
                         queue)))

(defmethod ig/halt-key! ::cache-thread-pool [k thread-pool]
  (.shutdown thread-pool))

(def rdf-formats [RDFFormat/BINARY
                  RDFFormat/NTRIPLES
                  RDFFormat/RDFXML
                  RDFFormat/RDFJSON
                  RDFFormat/TURTLE])

(def tuple-formats [TupleQueryResultFormat/BINARY
                    TupleQueryResultFormat/SPARQL
                    TupleQueryResultFormat/JSON])

(def boolean-formats [BooleanQueryResultFormat/TEXT
                      BooleanQueryResultFormat/JSON
                      BooleanQueryResultFormat/SPARQL])

;; TODO:
;;
;; We should turn these whitelist sets into proper configuration.
;;
;; Set some whitelists that ensure we're much more strict around what
;; formats we negotiate with stardog.  If you want to run drafter
;; against another (non stardog) store we should configure these to be
;; different.
;;
;; For construct we avoid Turtle because of Stardog bug #3087
;; (https://complexible.zendesk.com/hc/en-us/requests/524)
;;
;; Also we avoid RDF+XML because RDF+XML can't even represent some RDF graphs:
;; https://www.w3.org/TR/REC-rdf-syntax/#section-Serialising which
;; causes us some issues when URI's for predicates contain parentheses.
;;
;; Ideally we'd just run with sesame's defaults, but providing a
;; smaller list should mean less bugs in production as we can choose
;; the most reliable formats and avoid those with known issues.
;;
(def construct-formats-whitelist #{TurtleParserFactory NTriplesParserFactory NQuadsParserFactory TriGParserFactory BinaryRDFParserFactory})
(def select-formats-whitelist #{SPARQLResultsXMLParserFactory SPARQLResultsJSONParserFactory BinaryQueryResultParserFactory})
(def ask-formats-whitelist #{SPARQLBooleanJSONParserFactory BooleanTextParserFactory SPARQLBooleanXMLParserFactory})



(defn- build-format-keyword->format-map [formats]
  "Builds a hashmap from format keywords to RDFFormat's. 

  e.g. nt => RDFFormat/NTRIPLES etc..."
  (reduce (fn [acc fmt]
            (merge acc
                   (zipmap (map keyword (.getFileExtensions fmt))
                           (repeat fmt))))
          {}
          formats))

(def supported-cache-formats
  {:graph (build-format-keyword->format-map rdf-formats)
   :tuple (build-format-keyword->format-map tuple-formats)
   :boolean (build-format-keyword->format-map boolean-formats)})



(defprotocol Stash
  ;; This will return a nil on cache miss, or a Background[Graph|Tuple]Result
  ;;or a boolean on hit, depending on the query-type.
  (get-result [this cache-key base-uri-str])
  ;; Wrap the query-result to insert it into the cache as it is being read by the caller
  (wrap-result [this cache-key query-result])
  ;; Read into the handler, return nil to indicate a cache miss
  (async-read [this cache-key handler base-uri-str])
  ;; Wrap the handler, putting the async thing into the cache as it is being handled
  (wrap-async-handler [this cache-key async-handler]))

(defn dataset->graphs
  "Extract graphs from the dataset"
  [?dataset]
  (when ?dataset
    {:default-graphs (.getDefaultGraphs ?dataset)
     :named-graphs (.getNamedGraphs ?dataset)}))

(defn graphs->edn
  "Convert Graphs objects into a clojure value with consistent print
  order, so it's suitable for hashing."
  [{:keys [default-graphs named-graphs] :as graphs}]
  {:default-graphs (set (map str default-graphs))
   :named-graphs (set (map str named-graphs))})

(defn is-state-graph? [dataset]
  (some #{(URI. "http://publishmydata.com/graphs/drafter/drafts")}
        (concat (.getDefaultGraphs dataset) (.getNamedGraphs dataset))))

(defn fetch-modified-state [conn {:keys [named-graphs default-graphs] :as graphs}]
  (let [values {:graph (set (concat default-graphs named-graphs))}]
    (->> (first (doall (sparql/query "drafter/stasher/modified-state.sparql" values conn)))
         (remove (comp nil? second))
         (into {}))))

(defn- use-state-graph-key? [dataset]
  (or (nil? dataset)
      (is-state-graph? dataset)))

(defn generate-cache-key [query-type query-str ?dataset conn]
  (let [graphs (dataset->graphs ?dataset)
        modified-state (fetch-modified-state conn graphs)]
    {:dataset (graphs->edn graphs)
     :query-type query-type
     :query-str query-str
     :modified-times modified-state}))


(defn generate-state-graph-cache-key [query-type query-str ?dataset state-graph-modified-time]
  (let [graphs (dataset->graphs ?dataset)]
    {:dataset (graphs->edn graphs)
     :query-type query-type
     :query-str query-str
     :modified-time state-graph-modified-time}))

(defn generate-drafter-cache-key [state-graph-modified-time query-type _cache query-str ?dataset conn]
  {:pre [(or (instance? Dataset ?dataset)
             (nil? ?dataset))
         (instance? RepositoryConnection conn)]
   :post [(or (s/valid? ::ck/cache-key %)
              (s/valid? ::ck/state-graph-cache-key %))]}
  (or (and (use-state-graph-key? ?dataset)
           (generate-state-graph-cache-key query-type query-str ?dataset state-graph-modified-time))
      (generate-cache-key query-type query-str ?dataset conn)))

(defn get-charset [format]
  {:pre [(instance? org.eclipse.rdf4j.common.lang.FileFormat format)]}
  (when-not (#{RDFFormat/BINARY TupleQueryResultFormat/BINARY} format)
    (Charset/forName "UTF-8")))

(defn get-parser
  "Given a query type and format, find an RDF4j file format parser for that
  format."
  [query-type fmt-kw]
  {:pre [(some #{query-type} (keys supported-cache-formats))
         (keyword? fmt-kw)]}
  (let [fmt (get-in supported-cache-formats [query-type fmt-kw])
        parser (when-let [parser-factory (.orElse (condp instance? fmt
                                                    RDFFormat (.get (RDFParserRegistry/getInstance) fmt)
                                                    BooleanQueryResultFormat (.get (BooleanQueryResultParserRegistry/getInstance) fmt)
                                                    TupleQueryResultFormat (.get (TupleQueryResultParserRegistry/getInstance) fmt)
                                                    (java.util.Optional/empty))
                                                  nil)]
                 (.getParser parser-factory))]
    (assert parser (str "Error could not find a parser for format:" fmt))
    parser))

(defn- wrap-graph-result [bg-graph-result format stream]
  (let [prefixes (.getNamespaces bg-graph-result) ;; take prefixes from supplied result
        cache-file-writer ^RDFWriter (grafter.rdf4j.io/rdf-writer
                                      stream
                                      :format format
                                      :prefixes prefixes)]
    (.startRDF cache-file-writer)

    (reify GraphQueryResult
      (getNamespaces [this]
        prefixes)
      (close [this]
        (try
          (log/infof "Result finished, closing graph writer")
          (.close bg-graph-result)
          (.endRDF cache-file-writer)
          (.close stream)
          (catch Throwable ex
            (fc/cancel-and-close stream)
            (throw ex))))
      (hasNext [this]
        (try
          (.hasNext bg-graph-result)
          (catch Throwable ex
            (fc/cancel-and-close stream)
            (throw ex))))
      (next [this]
        (try
          (let [quad (.next bg-graph-result)]
            (.handleStatement cache-file-writer quad)
            quad)
          (catch Throwable ex
            (fc/cancel-and-close stream)
            (throw ex))))
      (remove [this]
        (try
          (.remove bg-graph-result)
          (catch Throwable ex
            (fc/cancel-and-close stream)
            (throw ex)))))))

(defn- wrap-tuple-result-pull [^TupleQueryResult bg-tuple-result fmt stream]
  (let [bindings (.getBindingNames bg-tuple-result)
        tuple-format (get-in supported-cache-formats [:tuple fmt])
        cache-file-writer ^TupleQueryResultWriter (QueryResultIO/createTupleWriter tuple-format stream)]
    (.startQueryResult cache-file-writer bindings)
    ;; pull interface
    (reify TupleQueryResult
      (getBindingNames [this]
        (.getBindingNames bg-tuple-result))
      (close [this]
        (log/infof "Result closed, closing tuple writer")
        (try
          (.close bg-tuple-result)
          (if (.hasNext this)
            (do (log/warn "Trying to close query result before consuming. Not writing cache")
                (fc/cancel-and-close stream))
            (do (.endQueryResult cache-file-writer)
                (.close stream)))
          (catch Throwable ex
            (.printStackTrace ex)
            (fc/cancel-and-close stream)
            (throw ex))))
      (hasNext [this]
        (try
          (.hasNext bg-tuple-result)
          (catch Throwable ex
            (fc/cancel-and-close stream)
            (throw ex))))
      (next [this]
        (try
          (let [solution (.next bg-tuple-result)]
            (.handleSolution cache-file-writer solution)
            solution)
          (catch Throwable ex
            (fc/cancel-and-close stream)
            (throw ex)))))))

(defn- wrap-tuple-result-push
  [^TupleQueryResultHandler result-handler fmt stream]
  (let [tuple-format (get-in supported-cache-formats [:tuple fmt])
        cache-file-writer ^TupleQueryResultWriter (QueryResultIO/createTupleWriter tuple-format stream)]
    (reify
      ;; Push interface...
      TupleQueryResultHandler
      (endQueryResult [this]
        (log/infof "Result ended, closing tuple writer")
        (.endQueryResult cache-file-writer)
        (.endQueryResult result-handler)
        (.close stream))
      (handleLinks [this links]
        (.handleLinks cache-file-writer links)
        (.handleLinks result-handler links))
      (handleBoolean [this bool]
        (.handleBoolean cache-file-writer bool)
        (.handleBoolean result-handler bool))
      (handleSolution [this binding-set]
        (.handleSolution cache-file-writer binding-set)
        (.handleSolution result-handler binding-set))
      (startQueryResult [this binding-names]
        (.startQueryResult cache-file-writer binding-names)
        (.startQueryResult result-handler binding-names)))))

(defn- stash-boolean-result [result fmt-kw stream]
  {:post [(some? %)]}
  ;; return the actual result this will get returned to the
  ;; outer-most call to query.  NOTE that boolean's are different to
  ;; other query types as they don't have a background-evaluator.
  (let [format (get-in supported-cache-formats [:boolean fmt-kw])]
    (with-open [stream stream]
      (let [bool-writer (QueryResultIO/createBooleanWriter format stream)]
        ;; Write the result to the file
        (.handleBoolean bool-writer result))))
  result)

(defn- read-tuple-cache-stream
  "Return a BackgroundTupleResult and trigger a thread to iterate over
  the result stream.  BackgroundGraphResult will then marshal the
  events through an iterator-like blocking interface.

  NOTE: there is no need to handle the RDF4j \"dataset\" as cache hits
  will already be on results where the dataset restriction was set."
  [thread-pool stream fmt]
  (let [start-time (System/currentTimeMillis)
        bg-tuple-result (BackgroundTupleResult. (get-parser :tuple fmt) stream)]

    ;; execute parse thread on a thread pool.
    (.submit thread-pool ^Runnable (fn []
                                     (try
                                       (.run bg-tuple-result)
                                       (dd/histogram! "drafter.stasher.tuple_sync.cache_hit"
                                                      (- (System/currentTimeMillis) start-time))
                                       (catch Throwable ex
                                         (log/warn ex "Error reading data from cache")))))
    bg-tuple-result))


(defn- read-graph-cache-stream
  "Return a BackgroundGraphResult and trigger a thread to iterate over
  the result stream.  BackgroundGraphResult will then marshal the
  events through an iterator-like blocking interface.

  NOTE: there is no need to handle the RDF4j \"dataset\" as cache hits
  will already be on results where the dataset restriction was set."
  [thread-pool base-uri-str stream fmt-kw]
  (let [start-time (System/currentTimeMillis)
        fmt (get-in supported-cache-formats [:graph fmt-kw])
        charset (get-charset fmt)
        bg-graph-result (BackgroundGraphResult. (get-parser :graph fmt-kw)
                                                stream
                                                charset
                                                base-uri-str)]

    ;; execute parse thread on a thread pool.
    (.submit thread-pool ^Runnable (fn []
                                     (try
                                       (.run bg-graph-result)
                                       (dd/histogram! "drafter.stasher.graph_sync.cache_hit"
                                                      (- (System/currentTimeMillis) start-time))
                                       (catch Throwable ex
                                         (log/warn ex "Error reading data from cache")))))

    bg-graph-result))

(defn- async-read-graph-cache-stream [stream fmt rdf-handler base-uri]
  {:post [(some? %)]}
  (dd/measure!
   "drafter.stasher.graph_async.cache_hit"
   {}
   (let [parser (get-parser :graph fmt)]
     (doto parser
       (.setRDFHandler rdf-handler)
       (.parse stream base-uri)))))

(defn- async-read-tuple-cache-stream [stream fmt tuple-handler]
  {:post [(some? %)]}
  (dd/measure!
   "drafter.stasher.tuple_async.cache_hit"
   {}
   (let [parser (get-parser :tuple fmt)]
     (doto parser
       (.setQueryResultHandler tuple-handler)
       (.parse stream)))))

(defn- wrap-graph-async-handler [inner-rdf-handler fmt out-stream]
  "Wrap an RDFHandler with one that will write the stream of RDF into
   the cache

  For RDF push query results."
  (let [rdf-format  (get-in supported-cache-formats [:graph fmt])
        ;; explicitly set prefixes to nil as gio/rdf-writer will write
        ;; the grafter default-prefixes otherwise.  By setting to nil,
        ;; use what comes from the stream instead.
        cache-file-writer ^RDFWriter (gio/rdf-writer out-stream :format rdf-format :prefixes nil)]
    (reify RDFHandler
      (startRDF [this]
        (try
          (.startRDF cache-file-writer)
          (.startRDF inner-rdf-handler)
          (catch Throwable ex
            (fc/cancel-and-close out-stream)
            (throw ex))))
      (endRDF [this]
        (try
          (.endRDF cache-file-writer)
          (.endRDF inner-rdf-handler)
          (.close out-stream)
          (catch Throwable ex
            (fc/cancel-and-close out-stream)
            (throw ex))))
      (handleStatement [this statement]
        (try
          (.handleStatement cache-file-writer statement)
          (.handleStatement inner-rdf-handler statement)
          (catch Throwable ex
            (fc/cancel-and-close out-stream)
            (throw ex))))
      (handleComment [this comment]
        (try
          (.handleComment cache-file-writer comment)
          (.handleComment inner-rdf-handler comment)
          (catch Throwable ex
            (fc/cancel-and-close out-stream)
            (throw ex))))
      (handleNamespace [this prefix-str uri-str]
        (try
          (.handleNamespace cache-file-writer prefix-str uri-str)
          (.handleNamespace inner-rdf-handler prefix-str uri-str)
          (catch Throwable ex
            (fc/cancel-and-close out-stream)
            (throw ex)))))))

(defn data-format [formats cache-key]
  {:post [(keyword? %)]}
  (get formats (ck/query-type cache-key)))

(defrecord StasherCache [cache-backend thread-pool formats]
  Stash
  (get-result [this cache-key base-uri-str]
    (let [fmt (data-format formats cache-key)]
      (when-let [in-stream (fc/source-stream cache-backend cache-key fmt)]
        (log/debugf "Found entry in cache for %s query" (ck/query-type cache-key))
        (case (ck/query-type cache-key)
          :graph (read-graph-cache-stream thread-pool base-uri-str in-stream fmt)
          :tuple (read-tuple-cache-stream thread-pool in-stream fmt)
          :boolean (dd/measure!
                    "drafter.stasher.boolean_sync.cache_hit"
                    {}
                    (.parse (get-parser :boolean fmt) in-stream)) ))))
  (wrap-result [this cache-key query-result]
    (let [fmt (data-format formats cache-key)
          out-stream (fc/destination-stream cache-backend cache-key fmt)]
      (log/debugf "Preparing to insert entry for %s query" (ck/query-type cache-key))
      (case (:query-type cache-key)
        :graph (timing/graph-result
                "drafter.stasher.graph_sync.cache_miss"
                (wrap-graph-result query-result fmt out-stream))
        :tuple (timing/tuple-result
                "drafter.stasher.tuple_sync.cache_miss"
                (wrap-tuple-result-pull query-result fmt out-stream))
        :boolean (stash-boolean-result query-result fmt out-stream))))
  (async-read [this cache-key handler base-uri-str]
    (let [fmt (data-format formats cache-key)]
      (when-let [in-stream (fc/source-stream cache-backend cache-key fmt)]
        (log/debugf "Found entry in cache for %s query" (ck/query-type cache-key))
        (case (:query-type cache-key)
          :graph (async-read-graph-cache-stream in-stream fmt handler base-uri-str)
          :tuple (async-read-tuple-cache-stream in-stream fmt handler))
        :hit)))
  (wrap-async-handler [this cache-key handler]
    (let [fmt (data-format formats cache-key)
          out-stream (fc/destination-stream cache-backend cache-key fmt)]
      (case (:query-type cache-key)
        :graph (timing/rdf-handler
                "drafter.stasher.graph_async.cache_miss"
                (wrap-graph-async-handler handler fmt out-stream))
        :tuple (timing/tuple-handler
                "drafter.stasher.tuple_async.cache_miss"
                (wrap-tuple-result-push handler fmt out-stream))))))

(defn stashing-graph-query
  "Construct a graph query that checks the stash before evaluating"
  [conn httpclient cache query-str base-uri-str {:keys [thread-pool cache?] :as opts}]
  (proxy [SPARQLGraphQuery] [httpclient base-uri-str query-str]
    (evaluate
      ;; sync results
      ([]
       (let [dataset (.getDataset this)]
         (if cache?
           (let [cache-key (generate-drafter-cache-key @(:state-graph-modified-time opts) :graph cache query-str dataset conn)]
             (or (get-result cache cache-key base-uri-str)
                 (wrap-result cache cache-key
                              (.sendGraphQuery httpclient QueryLanguage/SPARQL
                                               query-str base-uri-str dataset
                                               (.getIncludeInferred this) (.getMaxExecutionTime this)
                                               (.getBindingsArray this)))))
           (timing/graph-result
            "drafter.stasher.graph_sync.no_cache"
            (.sendGraphQuery httpclient QueryLanguage/SPARQL
                                                       query-str base-uri-str dataset
                                                       (.getIncludeInferred this) (.getMaxExecutionTime this)
                                                       (.getBindingsArray this))))))

      ;; async results
      ([rdf-handler]
       (let [dataset (.getDataset this)]
         (if cache?
           (let [cache-key (generate-drafter-cache-key @(:state-graph-modified-time opts) :graph cache query-str dataset conn)]
             (or (async-read cache cache-key rdf-handler base-uri-str)
                 (let [stashing-rdf-handler (wrap-async-handler cache cache-key rdf-handler)]
                   (.sendGraphQuery httpclient QueryLanguage/SPARQL
                                    query-str base-uri-str dataset
                                    (.getIncludeInferred this) (.getMaxExecutionTime this)
                                    stashing-rdf-handler (.getBindingsArray this)))))
           (let [timing-rdf-handler (timing/rdf-handler "drafter.stasher.graph_async.no_cache" rdf-handler)]
             (.sendGraphQuery httpclient QueryLanguage/SPARQL
                              query-str base-uri-str dataset
                              (.getIncludeInferred this) (.getMaxExecutionTime this)
                              timing-rdf-handler (.getBindingsArray this)))))))))

(defn stashing-select-query
  "Construct a tuple query that checks the stash before evaluating"
  [conn httpclient cache query-str base-uri-str {:keys [thread-pool cache?] :as opts}]
  (proxy [SPARQLTupleQuery] [httpclient base-uri-str query-str]
    (evaluate
      ;; sync results
      ([]
       (let [dataset (.getDataset this)]
         (if cache?
           (let [cache-key (generate-drafter-cache-key @(:state-graph-modified-time opts) :tuple cache query-str dataset conn)]
             (or (get-result cache cache-key base-uri-str)
                 (wrap-result cache cache-key
                              (.sendTupleQuery httpclient QueryLanguage/SPARQL
                                               query-str base-uri-str dataset
                                               (.getIncludeInferred this)
                                               (.getMaxExecutionTime this)
                                               (.getBindingsArray this)))))
           (timing/tuple-result
            "drafter.stasher.tuple_sync.no_cache"
            (.sendTupleQuery httpclient QueryLanguage/SPARQL
                             query-str base-uri-str dataset
                             (.getIncludeInferred this)
                             (.getMaxExecutionTime this)
                             (.getBindingsArray this))))))
      ([tuple-handler]
       (let [dataset (.getDataset this)]
         (if cache?
           (let [cache-key (generate-drafter-cache-key @(:state-graph-modified-time opts) :tuple cache query-str dataset conn)]
             (or (async-read cache cache-key tuple-handler base-uri-str)
                 (let [stashing-tuple-handler (wrap-async-handler cache cache-key tuple-handler)]
                   (.sendTupleQuery httpclient QueryLanguage/SPARQL
                                    query-str base-uri-str dataset
                                    (.getIncludeInferred this) (.getMaxExecutionTime this)
                                    stashing-tuple-handler (.getBindingsArray this)))))
           (let [timing-tuple-handler (timing/tuple-handler "drafter.stasher.tuple_async.no_cache" tuple-handler)]
             (.sendTupleQuery httpclient QueryLanguage/SPARQL
                              query-str base-uri-str dataset
                              (.getIncludeInferred this) (.getMaxExecutionTime this)
                              timing-tuple-handler (.getBindingsArray this)))))))))

(defn stashing-boolean-query
  "Construct a boolean query that checks the stash before evaluating.
  Boolean queries are sync only"
  [conn httpclient cache query-str base-uri-str {:keys [thread-pool cache?] :as opts}]
  (proxy [SPARQLBooleanQuery] [httpclient query-str base-uri-str]
    (evaluate []
      (let [dataset (.getDataset this)]
        (if cache? 
          (let [cache-key (generate-drafter-cache-key @(:state-graph-modified-time opts) :boolean cache query-str dataset conn)
                result (get-result cache cache-key base-uri-str)]
            (if (some? result)
              result
              (dd/measure!
               "drafter.stasher.boolean_sync.cache_miss"
               {}
               (wrap-result cache cache-key
                            (.sendBooleanQuery httpclient QueryLanguage/SPARQL
                                               query-str base-uri-str dataset
                                               (.getIncludeInferred this)
                                               (.getMaxExecutionTime this)
                                               (.getBindingsArray this))))))
          (dd/measure!
           "drafter.stasher.boolean_sync.no_cache"
           {}
           (.sendBooleanQuery httpclient QueryLanguage/SPARQL
                              query-str base-uri-str dataset
                              (.getIncludeInferred this)
                              (.getMaxExecutionTime this)
                              (.getBindingsArray this))))))))

(defn- get-state-graph-modified-time []
  (let [state-graph-modified-time (java.util.Date.)]
    (log/infof "Using new state graph modified time of: %s" state-graph-modified-time)
    state-graph-modified-time))

(defn cache-busting-update-statement
  [httpclient query-str base-uri-str state-graph-modified-time]
  (proxy [SPARQLUpdate] [httpclient base-uri-str query-str]
    (execute []
      (proxy-super execute)
      (reset! state-graph-modified-time (get-state-graph-modified-time)))))


(defn- stasher-connection [repo httpclient cache {:keys [quad-mode base-uri] :or {quad-mode false} :as opts}]
  (proxy [DrafterSPARQLConnection] [repo httpclient quad-mode]

    (commit []
      (proxy-super commit)
      (reset! (:state-graph-modified-time opts) (get-state-graph-modified-time)))
    (prepareUpdate [_ query-str base-uri-str]
      (cache-busting-update-statement httpclient query-str (or base-uri-str base-uri) (:state-graph-modified-time opts)))

    (prepareTupleQuery [_ query-str base-uri-str]
      (stashing-select-query this httpclient cache query-str (or base-uri-str base-uri) opts))

    (prepareGraphQuery [_ query-str base-uri-str]
      (stashing-graph-query this httpclient cache query-str (or base-uri-str base-uri) opts))

    (prepareBooleanQuery [_ query-str base-uri-str]
      (stashing-boolean-query this httpclient cache query-str (or base-uri-str base-uri) opts))))

(defn stasher-repo
  "Builds a stasher RDF repository, that implements the standard RDF4j
  repository interface but caches query results to disk and
  transparently returns cached results if they're in the cache."
  [{:keys [sparql-query-endpoint sparql-update-endpoint report-deltas cache] :as opts}]
  ;; This call here obliterates the sesame defaults for registered
  ;; parsers.  Forcing content negotiation to work only with the
  ;; parsers we explicitly whitelist above.
  (reg/register-parser-factories! {:select select-formats-whitelist
                                   :construct construct-formats-whitelist
                                   :ask ask-formats-whitelist})
  (let [query-endpoint (str sparql-query-endpoint)
        update-endpoint (str sparql-update-endpoint)
        deltas (boolean (or report-deltas true))

        updated-opts (assoc opts
                            :cache? (get opts :cache? true)
                            :base-uri (or (:base-uri opts)
                                          "http://publishmydata.com/id/")
                            :state-graph-modified-time (atom (get-state-graph-modified-time)))
        repo (doto (proxy [DrafterSPARQLRepository] [query-endpoint update-endpoint]
                     (getConnection []
                       (stasher-connection this (.createHTTPClient this) cache updated-opts)))
               (.initialize))]
    (log/info "Initialised repo at QUERY=" query-endpoint ", UPDATE=" update-endpoint)
    (repo/notifying-repo repo deltas)))

(defn stasher-cache [opts]
  (let [default-formats {:boolean :txt
                         :tuple :brt
                         :graph :brf}
        opts (assoc opts :formats (merge default-formats (:formats opts)))]
    (map->StasherCache opts)))

(defmethod ig/init-key :drafter.stasher/repo [_ opts]
  (stasher-repo opts))

(defmethod ig/halt-key! :drafter.stasher/repo [_ repo]
  (log/info "Shutting down stasher repo")
  (drpr/stop-backend repo))


(defmethod ig/init-key :drafter.stasher/cache [_ opts]
  (stasher-cache opts))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Specs

(s/def ::sparql-query-endpoint uri?)
(s/def ::sparql-update-endpoint uri?)
(s/def ::thread-pool #(instance? java.util.concurrent.ThreadPoolExecutor %))

(defmethod ig/pre-init-spec :drafter.stasher/repo [_]
  (s/keys :req-un [::sparql-query-endpoint ::sparql-update-endpoint ::cache]
          :opt-un [::quad-mode ::report-deltas ::base-uri ::cache?]))

(defmethod ig/pre-init-spec :drafter.stasher/cache [_]
  (s/keys :req-un [::cache-backend ::thread-pool]
          :opt-un [::formats]))


