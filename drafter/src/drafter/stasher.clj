(ns drafter.stasher
  (:require
   [clojure.spec.alpha :as s]
   [clojure.spec.gen.alpha :as g]
   [clojure.tools.logging :as log]
   [cognician.dogstatsd :as dd]
   [drafter.stasher.cache-key :as ck]
   [drafter.stasher.cancellable :as c]
   [drafter.stasher.filecache :as fc]
   [drafter.stasher.formats :as formats]
   [drafter.stasher.timing :as timing]
   [drafter.util :as util]
   [grafter-2.rdf4j.io :as rio]
   [grafter-2.rdf4j.repository :as repo]
   [grafter-2.rdf4j.repository.registry :as reg]
   [grafter-2.rdf4j.sparql :as sparql]
   [integrant.core :as ig])
  (:import java.net.URI
           java.nio.charset.Charset
           org.eclipse.rdf4j.query.impl.BackgroundGraphResult
           (org.eclipse.rdf4j.query Dataset GraphQueryResult QueryLanguage
                                    TupleQueryResultHandler TupleQueryResult)
           org.eclipse.rdf4j.query.resultio.helpers.BackgroundTupleResult
           (org.eclipse.rdf4j.query.resultio TupleQueryResultFormat BooleanQueryResultFormat QueryResultIO
                                             TupleQueryResultWriter BooleanQueryResultParserRegistry
                                             TupleQueryResultParserRegistry TupleQueryResultParser BooleanQueryResultParser
                                             TupleQueryResultParserFactory
                                             BooleanQueryResultParserFactory)
           (org.eclipse.rdf4j.repository RepositoryConnection)
           (org.eclipse.rdf4j.repository.sparql SPARQLRepository SPARQLConnection)
           org.eclipse.rdf4j.http.client.SPARQLProtocolSession
           (org.eclipse.rdf4j.repository.sparql.query SPARQLBooleanQuery SPARQLGraphQuery SPARQLTupleQuery SPARQLUpdate QueryStringUtil)
           (org.eclipse.rdf4j.rio RDFParser RDFFormat RDFHandler RDFWriter RDFParserRegistry RDFParserFactory)
           (java.util.concurrent ThreadPoolExecutor TimeUnit ArrayBlockingQueue)
           java.time.OffsetDateTime
           (java.io InputStream Closeable)))

(s/def ::core-pool-size pos-int?)
(s/def ::max-pool-size pos-int?)
(s/def ::keep-alive-time-ms integer?)
(s/def ::queue-size pos-int?)

(defmethod ig/pre-init-spec ::cache-thread-pool [_]
  (s/keys :req-un [::core-pool-size ::max-pool-size ::keep-alive-time-ms
                   ::queue-size]))

;; This threadpool will raise a java.util.concurrent.RejectedExecutionException
;; when there are no free threads to execute tasks and the queue of
;; waiting jobs is full.
(defmethod ig/init-key ::cache-thread-pool [_ opts]
  (let [{:keys [core-pool-size
                max-pool-size
                keep-alive-time-ms ;; time threads above core-pool-size wait for work before dying.
                queue-size]} opts
        queue (ArrayBlockingQueue. queue-size)]
    (proxy [ThreadPoolExecutor] [core-pool-size
                                 max-pool-size
                                 keep-alive-time-ms
                                 TimeUnit/MILLISECONDS
                                 queue]
      (beforeExecute [t r]
        (let [^ThreadPoolExecutor this this]
          (proxy-super beforeExecute t r)
          (dd/histogram! "stasher_thread_pool_size" (.getPoolSize this)))))))

(defmethod ig/halt-key! ::cache-thread-pool [k ^ThreadPoolExecutor thread-pool]
  (.shutdown thread-pool))

(defprotocol Stash
  (get-result [this cache-key base-uri-str]
    "This will return a nil on cache miss, or a
    Background[Graph|Tuple]Result or a boolean on hit, depending on
    the query-type.")
  (wrap-result [this cache-key query-result]
    "Wrap the query-result to insert it into the cache as it is being read by the caller")
  (async-read [this cache-key handler base-uri-str]
    "Read into the handler, return nil to indicate a cache miss")
  (wrap-async-handler [this cache-key async-handler]
    "Wrap the handler, putting the async thing into the cache as it is being handled"))

(defn dataset->graphs
  "Extract graphs from the dataset"
  [^Dataset ?dataset]
  (when ?dataset
    {:default-graphs (.getDefaultGraphs ?dataset)
     :named-graphs (.getNamedGraphs ?dataset)}))

(defn graphs->edn
  "Convert Graphs objects into a clojure value with consistent print
  order, so it's suitable for hashing."
  [{:keys [default-graphs named-graphs] :as graphs}]
  {:default-graphs (set (map str default-graphs))
   :named-graphs (set (map str named-graphs))})

(defn is-state-graph? [^Dataset dataset]
  (some #{(URI. "http://publishmydata.com/graphs/drafter/drafts")}
        (concat (.getDefaultGraphs dataset) (.getNamedGraphs dataset))))

(defn- merge-draftver-and-livever [{:keys [draftver livever] :as m}]
  (let [m (dissoc m :draftver :livever)]
    (cond
      (and draftver livever)
      (assoc m :version (util/merge-versions draftver livever))

      (or draftver livever)
      (assoc m :version (or draftver livever))

      :else
      m)))

(defn fetch-last-modified [conn {:keys [named-graphs default-graphs] :as graphs}]
  (let [values {:graph (distinct (concat default-graphs named-graphs))}]
    (->> (first (doall (sparql/query "drafter/stasher/last-modified.sparql" values conn)))
         (remove (comp nil? second))
         (into {})
         merge-draftver-and-livever)))

(defn- use-state-graph-key? [dataset]
  (or (nil? dataset)
      (is-state-graph? dataset)))

(defn generate-cache-key [query-type query-str ?dataset conn]
  (let [graphs (dataset->graphs ?dataset)
        last-modified (fetch-last-modified conn graphs)]
    {:dataset (graphs->edn graphs)
     :query-type query-type
     :query-str query-str
     :last-modified last-modified}))


(defn generate-state-graph-cache-key
  [query-type query-str ?dataset state-graph-last-modified]
  (let [graphs (dataset->graphs ?dataset)]
    {:dataset (graphs->edn graphs)
     :query-type query-type
     :query-str query-str
     :state-graph-last-modified state-graph-last-modified}))

(defn generate-drafter-cache-key
  [state-graph-last-modified query-type _cache query-str ?dataset conn]
  (or (and (use-state-graph-key? ?dataset)
           (generate-state-graph-cache-key query-type
                                           query-str
                                           ?dataset
                                           state-graph-last-modified))
      (generate-cache-key query-type query-str ?dataset conn)))

(s/def ::dataset (s/with-gen (s/nilable #(instance? Dataset %))
                   #(g/frequency [[1 (g/return nil)]
                                  [1 (g/return (org.eclipse.rdf4j.query.impl.DatasetImpl.))]])))

(s/def ::connection (s/with-gen #(instance? RepositoryConnection %)
                      #(g/return
                        (repo/->connection (repo/sparql-repo "http://localhost:1234/dummy-connection/query")))))

(s/fdef generate-drafter-cache-key
  :args (s/cat :state-graph-last-modified :drafter.stasher.cache-key/state-graph-last-modified
               :query-type :drafter.stasher.cache-key/query-type
               :cache any?
               :query-str string?
               :dataset ::dataset
               :conn ::connection)
  :ret :drafter.stasher.cache-key/either-cache-key)

(defn get-charset [format]
  {:pre [(instance? org.eclipse.rdf4j.common.lang.FileFormat format)]}
  (when-not (#{RDFFormat/BINARY TupleQueryResultFormat/BINARY} format)
    (Charset/forName "UTF-8")))

(defprotocol GetParser
  (get-parser* [fmt]))

(extend-protocol GetParser
  RDFFormat
  (get-parser* [fmt]
    (when-let [^RDFParserFactory parser-factory (.orElse (.get (RDFParserRegistry/getInstance) fmt) nil)]
      (.getParser parser-factory)))

  BooleanQueryResultFormat
  (get-parser* [fmt]
    (when-let [^BooleanQueryResultParserFactory parser-factory (.orElse (.get (BooleanQueryResultParserRegistry/getInstance) fmt) nil)]
      (.getParser parser-factory)))

  TupleQueryResultFormat
  (get-parser* [fmt]
    (when-let [^TupleQueryResultParserFactory parser-factory (.orElse (.get (TupleQueryResultParserRegistry/getInstance) fmt) nil)]
      (.getParser parser-factory))))

(defn get-parser
  "Given a query type and format, find an RDF4j file format parser for that
  format."
  [query-type fmt-kw]
  (let [fmt (get-in formats/supported-cache-formats [query-type fmt-kw])
        parser (get-parser* fmt)]
    (assert parser (str "Error could not find a parser for format:" fmt))
    parser))

(s/fdef get-parser
  :args (s/with-gen
          (s/cat :query-type :drafter.stasher.formats/query-type-keyword
                 :format :drafter.stasher.formats/cache-format-keyword)
          #(g/fmap
            (fn [qt]
              [qt (rand-nth (keys (formats/supported-cache-formats qt)))])
            (s/gen #{:graph :tuple :boolean})))

  :ret (s/or :rdf-parser #(instance? RDFParser %)
             :tuple-parser #(instance? TupleQueryResultParser %)
             :boolean-parser #(instance? BooleanQueryResultParser %)))

(defn- wrap-graph-result [^GraphQueryResult bg-graph-result format ^Closeable stream]
  (let [prefixes (.getNamespaces bg-graph-result) ;; take prefixes from supplied result
        cache-file-writer ^RDFWriter (rio/rdf-writer
                                      stream
                                      :format format
                                      :prefixes prefixes)]
    (.startRDF cache-file-writer)

    (reify GraphQueryResult
      (getNamespaces [this]
        prefixes)
      (close [this]
        (try
          (log/tracef "Result finished, closing graph writer")
          (.close bg-graph-result)
          (.endRDF cache-file-writer)
          (catch Throwable ex
            (c/cancel stream)
            (throw ex))
          (finally
            (.close stream))))
      (hasNext [this]
        (try
          (.hasNext bg-graph-result)
          (catch Throwable ex
            (c/cancel stream)
            (.close stream)
            (throw ex))))
      (next [this]
        (try
          (let [quad (.next bg-graph-result)]
            (.handleStatement cache-file-writer quad)
            quad)
          (catch Throwable ex
            (c/cancel stream)
            (.close stream)
            (throw ex))))
      (remove [this]
        (try
          (.remove bg-graph-result)
          (catch Throwable ex
            (c/cancel stream)
            (.close stream)
            (throw ex)))))))

(defn- wrap-tuple-result-pull [^TupleQueryResult bg-tuple-result fmt ^Closeable stream]
  (let [bindings (.getBindingNames bg-tuple-result)
        tuple-format (get-in formats/supported-cache-formats [:tuple fmt])
        cache-file-writer ^TupleQueryResultWriter (QueryResultIO/createTupleWriter tuple-format stream)]
    (.startQueryResult cache-file-writer bindings)
    ;; pull interface
    (reify TupleQueryResult
      (getBindingNames [this]
        (.getBindingNames bg-tuple-result))
      (close [this]
        (log/tracef "Result closed, closing tuple writer")
        (try
          (.close bg-tuple-result)
          (if (.hasNext this)
            (do (log/warn "Trying to close query result before consuming. Not writing cache")
                (c/cancel stream))
            (.endQueryResult cache-file-writer))
          (catch Throwable ex
            (c/cancel stream)
            (throw ex))
          (finally
            (.close stream))))
      (hasNext [this]
        (try
          (.hasNext bg-tuple-result)
          (catch Throwable ex
            (c/cancel stream)
            (.close stream)
            (throw ex))))
      (next [this]
        (try
          (let [solution (.next bg-tuple-result)]
            (.handleSolution cache-file-writer solution)
            solution)
          (catch Throwable ex
            (c/cancel stream)
            (.close stream)
            (throw ex)))))))

(defn- wrap-tuple-result-push
  [^TupleQueryResultHandler result-handler fmt ^Closeable stream]
  (let [tuple-format (get-in formats/supported-cache-formats [:tuple fmt])
        cache-file-writer ^TupleQueryResultWriter (QueryResultIO/createTupleWriter tuple-format stream)]
    (reify
      ;; Push interface...
      TupleQueryResultHandler
      (endQueryResult [this]
        (.endQueryResult cache-file-writer)
        (.endQueryResult result-handler))
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
        (.startQueryResult result-handler binding-names))

      Closeable
      (close [t]
        (.close stream))

      c/Cancellable
      (cancel [_]
        (c/cancel stream)))))

(defn- stash-boolean-result [result fmt-kw ^Closeable stream]
  {:post [(some? %)]}
  ;; return the actual result this will get returned to the
  ;; outer-most call to query.  NOTE that boolean's are different to
  ;; other query types as they don't have a background-evaluator.
  (let [format (get-in formats/supported-cache-formats [:boolean fmt-kw])]
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
  [^ThreadPoolExecutor thread-pool stream fmt]
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
  [^ThreadPoolExecutor thread-pool base-uri-str stream fmt-kw]
  (let [start-time (System/currentTimeMillis)
        fmt (get-in formats/supported-cache-formats [:graph fmt-kw])
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

(defn- async-read-graph-cache-stream [^InputStream stream fmt rdf-handler ^String base-uri]
  {:post [(some? %)]}
  (dd/measure!
   "drafter.stasher.graph_async.cache_hit"
   {}
   (let [^RDFParser parser (get-parser :graph fmt)]
     (doto parser
       (.setRDFHandler rdf-handler)
       (.parse stream base-uri)))))

(defn- async-read-tuple-cache-stream [stream fmt tuple-handler]
  {:post [(some? %)]}
  (dd/measure!
   "drafter.stasher.tuple_async.cache_hit"
   {}
   (let [^TupleQueryResultParser parser (get-parser :tuple fmt)]
     (doto parser
       (.setQueryResultHandler tuple-handler)
       (.parse stream)))))

(defn- wrap-graph-async-handler
  "Wrap an RDFHandler with one that will write the stream of RDF into
   the cache

  For RDF push query results."
  [^RDFHandler inner-rdf-handler fmt ^Closeable out-stream]
  (let [rdf-format  (get-in formats/supported-cache-formats [:graph fmt])
        ;; explicitly set prefixes to nil as rio/rdf-writer will write
        ;; the grafter default-prefixes otherwise.  By setting to nil,
        ;; use what comes from the stream instead.
        cache-file-writer ^RDFWriter (rio/rdf-writer out-stream :format rdf-format :prefixes nil)]
    (reify
      RDFHandler
      (startRDF [this]
        (.startRDF cache-file-writer)
        (.startRDF inner-rdf-handler))
      (endRDF [this]
        (.endRDF cache-file-writer)
        (.endRDF inner-rdf-handler))
      (handleStatement [this statement]
         (.handleStatement cache-file-writer statement)
         (.handleStatement inner-rdf-handler statement))
      (handleComment [this comment]
        (.handleComment cache-file-writer comment)
        (.handleComment inner-rdf-handler comment))
      (handleNamespace [this prefix-str uri-str]
        (.handleNamespace cache-file-writer prefix-str uri-str)
        (.handleNamespace inner-rdf-handler prefix-str uri-str))

      Closeable
      (close [t]
        (.close out-stream))

      c/Cancellable
      (cancel [_]
        (c/cancel out-stream)))))

(defn data-format [formats cache-key]
  {:post [(keyword? %)]}
  (get formats (ck/query-type cache-key)))

(defn log-stasher-status
  "Specialised stasher logging procedure.

  We maintain two log files the \"drafter.log\" where the main
  narrative of requests are logged, and the \"sparql.log\" where we
  log sparql queries and sparql update statements.

  The \"drafter.log\" doesn't contain the sparql queries themselves,
  just the hashes of the queries and whether they were hits or misses.

  The \"sparql.log\" contains the queries along with duplicated
  information (from the \"drafter.log\") on their cache keys etc,
  however in our default configuration we log only SPARQL misses only,
  with SPARQL query hits being logged at debug level.

  In the default config and in the case of cache hits we should have
  logged the query already, so searching for the hash in the logs
  should be enough to discover the original query, and as most queries
  are cache hits this saves a huge amount of noise in the logs.

  However, the rotation policy on the SPARQL query log may mean that
  the original cache miss has been rotated out of the logs. In these
  cases production users can temporarily reconfigure the log4j2.xml
  file so that drafter.rdf.sparql is set to log at a 'debug' level and
  wait for the configuration to be reloaded. This should allow
  repeating the query to capture the query in the logs again."
  [hit-or-miss {:keys [query-str] :as cache-key}]
  (case hit-or-miss
    "miss" (do
             (log/log "drafter.stasher" ; log to standard drafter log (with elided query)
                      :info
                      nil
                      (format "Stasher miss key: %s for %s query (elided)"
                              (fc/cache-key->hash-key cache-key)
                              (name (ck/query-type cache-key))))
             (log/log "drafter.rdf.sparql" ; log from drafter.rdf.sparql ns (so queries are logged together)
                      :info
                      nil
                      (format "Stasher miss key: %s for %s query:\n%s"
                              (fc/cache-key->hash-key cache-key)
                              (name (ck/query-type cache-key))
                              query-str)))
    "hit" (let [hash-key (fc/cache-key->hash-key cache-key)
                query-type (name (ck/query-type cache-key))]
            ;; NOTE we log hits at info level without the query string
            (log/log "drafter.stasher"
                     :info
                     nil
                     (format "Stasher hit key: %s for %s query (elided)"
                             hash-key
                             query-type))
            ;; We log again but at debug level, so operators can
            ;; toggle debugging on temporarily to discover what the
            ;; problematic cached query is.
            (log/log "drafter.rdf.sparql" ; log from drafter.rdf.sparql ns (so queries are logged together)
                     :debug
                     nil
                     (format "Stasher hit key: %s for %s query:\n%s"
                             hash-key
                             query-type
                             query-str)))))

(defrecord StasherCache [cache-backend thread-pool formats]
  Stash
  (get-result [this cache-key base-uri-str]
    (let [fmt (data-format formats cache-key)]
      (when-let [^Closeable in-stream (fc/source-stream cache-backend cache-key fmt)]
        (log-stasher-status "hit" cache-key)
        (case (ck/query-type cache-key)
          :graph (read-graph-cache-stream thread-pool base-uri-str in-stream fmt)
          :tuple (read-tuple-cache-stream thread-pool in-stream fmt)
          :boolean (dd/measure!
                    "drafter.stasher.boolean_sync.cache_hit"
                    {}
                    (with-open [^Closeable is in-stream]
                      (let [^BooleanQueryResultParser parser (get-parser :boolean fmt)]
                        (.parse parser is))))))))
  (wrap-result [this cache-key query-result]
    (let [fmt (data-format formats cache-key)
          out-stream (fc/destination-stream cache-backend cache-key fmt)]
      (log-stasher-status "miss" cache-key)
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
        (log-stasher-status "hit" cache-key)
        (with-open [^Closeable input-stream in-stream]
          (case (:query-type cache-key)
            :graph (async-read-graph-cache-stream input-stream fmt handler base-uri-str)
            :tuple (async-read-tuple-cache-stream input-stream fmt handler)))
        :hit)))
  (wrap-async-handler [this cache-key handler]
    (let [fmt (data-format formats cache-key)
          out-stream (fc/destination-stream cache-backend cache-key fmt)]
      (log-stasher-status "miss" cache-key)
      (case (:query-type cache-key)
        :graph (timing/rdf-handler
                "drafter.stasher.graph_async.cache_miss"
                (wrap-graph-async-handler handler fmt out-stream))
        :tuple (timing/tuple-handler
                "drafter.stasher.tuple_async.cache_miss"
                (wrap-tuple-result-push handler fmt out-stream))))))

(defn stashing-graph-query
  "Construct a graph query that checks the stash before evaluating"
  [conn ^SPARQLProtocolSession httpclient cache query-str base-uri-str {:keys [thread-pool cache?] :as opts}]
  (proxy [SPARQLGraphQuery] [httpclient base-uri-str query-str]
    (evaluate
      ;; sync results
      ([]
       (let [^SPARQLGraphQuery this this
             ^Dataset dataset (.getDataset this)
             query-str (QueryStringUtil/getGraphQueryString query-str (.getBindings this))]
         (if cache?
           (let [cache-key (generate-drafter-cache-key @(:state-graph-last-modified opts)
                                                       :graph
                                                       cache
                                                       query-str
                                                       dataset
                                                       conn)]
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
       (let [^SPARQLGraphQuery this this
             dataset (.getDataset this)
             query-str (QueryStringUtil/getGraphQueryString query-str (.getBindings this))]
         (if cache?
           (let [cache-key (generate-drafter-cache-key @(:state-graph-last-modified opts)
                                                       :graph
                                                       cache
                                                       query-str
                                                       dataset
                                                       conn)]
             (or (async-read cache cache-key rdf-handler base-uri-str)
                 (c/with-open [^Closeable handler (wrap-async-handler
                                                   cache
                                                   cache-key
                                                   rdf-handler)]
                   (.sendGraphQuery httpclient QueryLanguage/SPARQL
                                    query-str base-uri-str dataset
                                    (.getIncludeInferred this)
                                    (.getMaxExecutionTime this)
                                    handler
                                    (.getBindingsArray this)))))
           (let [timing-rdf-handler (timing/rdf-handler "drafter.stasher.graph_async.no_cache" rdf-handler)]
             (.sendGraphQuery httpclient QueryLanguage/SPARQL
                              query-str base-uri-str dataset
                              (.getIncludeInferred this) (.getMaxExecutionTime this)
                              timing-rdf-handler (.getBindingsArray this)))))))))

(defn stashing-select-query
  "Construct a tuple query that checks the stash before evaluating"
  [conn ^SPARQLProtocolSession httpclient cache query-str base-uri-str {:keys [thread-pool cache?] :as opts}]
  (proxy [SPARQLTupleQuery] [httpclient base-uri-str query-str]
    (evaluate
      ;; sync results
      ([]
       (let [^SPARQLTupleQuery this this
             ^Dataset dataset (.getDataset this)
             query-str (QueryStringUtil/getTupleQueryString query-str (.getBindings this))]
         (if cache?
           (let [cache-key (generate-drafter-cache-key @(:state-graph-last-modified opts)
                                                       :tuple
                                                       cache
                                                       query-str
                                                       dataset
                                                       conn)]
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
       (let [^SPARQLTupleQuery this this
             dataset (.getDataset this)
             query-str (QueryStringUtil/getTupleQueryString query-str (.getBindings this))]
         (if cache?
           (let [cache-key (generate-drafter-cache-key @(:state-graph-last-modified opts)
                                                       :tuple
                                                       cache
                                                       query-str
                                                       dataset
                                                       conn)]
             (or (async-read cache cache-key tuple-handler base-uri-str)
                 (c/with-open [^Closeable handler (wrap-async-handler
                                                   cache
                                                   cache-key
                                                   tuple-handler)]
                   (.sendTupleQuery httpclient QueryLanguage/SPARQL
                                    query-str base-uri-str dataset
                                    (.getIncludeInferred this)
                                    (.getMaxExecutionTime this)
                                    handler
                                    (.getBindingsArray this)))))
           (let [timing-tuple-handler (timing/tuple-handler "drafter.stasher.tuple_async.no_cache" tuple-handler)]
             (.sendTupleQuery httpclient QueryLanguage/SPARQL
                              query-str base-uri-str dataset
                              (.getIncludeInferred this) (.getMaxExecutionTime this)
                              timing-tuple-handler (.getBindingsArray this)))))))))

(defn stashing-boolean-query
  "Construct a boolean query that checks the stash before evaluating.
  Boolean queries are sync only"
  [conn ^SPARQLProtocolSession httpclient cache query-str base-uri-str {:keys [thread-pool cache?] :as opts}]
  (proxy [SPARQLBooleanQuery] [httpclient query-str base-uri-str]
    (evaluate []
      (let [^SPARQLBooleanQuery this this
            dataset (.getDataset this)
            query-str (QueryStringUtil/getBooleanQueryString query-str (.getBindings this))]
        (if cache?
          (let [cache-key (generate-drafter-cache-key @(:state-graph-last-modified opts)
                                                      :boolean
                                                      cache
                                                      query-str
                                                      dataset
                                                      conn)
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

(defn- get-state-graph-last-modified []
  (let [time (OffsetDateTime/now)
        version (util/version)]
    (log/infof "Using new state graph last modified of: %s_%s" time version)
    {:time time
     :version version}))

(defn cache-busting-update-statement
  [httpclient query-str base-uri-str state-graph-last-modified]
  (proxy [SPARQLUpdate] [httpclient base-uri-str query-str]
    (execute []
      (let [^SPARQLUpdate this this]
        (proxy-super execute))
      (reset! state-graph-last-modified (get-state-graph-last-modified)))))


(defn- stasher-connection [repo httpclient cache {:keys [quad-mode base-uri] :or {quad-mode false} :as opts}]
  (proxy [SPARQLConnection] [repo httpclient quad-mode]

    (commit []
      (let [^SPARQLConnection this this]
        (proxy-super commit))
      (reset! (:state-graph-last-modified opts)
              (get-state-graph-last-modified)))
    (prepareUpdate [_ query-str base-uri-str]
      (cache-busting-update-statement httpclient
                                      query-str
                                      (or base-uri-str base-uri)
                                      (:state-graph-last-modified opts)))

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
  (reg/register-parser-factories! {:select formats/select-formats-whitelist
                                   :construct formats/construct-formats-whitelist
                                   :ask formats/ask-formats-whitelist})
  (let [query-endpoint (str sparql-query-endpoint)
        update-endpoint (str sparql-update-endpoint)
        deltas (boolean (or report-deltas true))

        updated-opts (assoc opts
                            :cache? (get opts :cache? true)
                            :base-uri (or (:base-uri opts)
                                          "http://publishmydata.com/id/")
                            :state-graph-last-modified (atom (get-state-graph-last-modified)))
        repo (doto (proxy [SPARQLRepository] [query-endpoint update-endpoint]
                     (getConnection []
                       (let [^SPARQLRepository this this
                             http-client (.createHTTPClient this)]
                         (stasher-connection this http-client cache updated-opts))))
               (.initialize))]
    (log/info "Initialised repo at QUERY=" query-endpoint ", UPDATE=" update-endpoint)
    (log/infof "Stasher Caching enabled: %b" (get updated-opts :cache?))
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
  (repo/shutdown repo))


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
