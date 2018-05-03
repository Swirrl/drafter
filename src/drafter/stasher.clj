(ns drafter.stasher
  (:require [clojure.core.cache :as cache]
            [clojure.java.io :as io]
            [clojure.spec.alpha :as s]
            [drafter.stasher.filecache :as fc]
            [drafter.stasher.cache-key :as ck]
            [grafter.rdf4j.repository :as repo]
            [grafter.rdf4j.sparql :as sparql]
            [integrant.core :as ig]
            [clojure.tools.logging :as log]
            [cognician.dogstatsd :as dd])
  (:import (drafter.rdf DrafterSPARQLConnection DrafterSPARQLRepository)
           java.nio.charset.Charset
           org.eclipse.rdf4j.query.impl.BackgroundGraphResult
           (org.eclipse.rdf4j.query Dataset GraphQueryResult QueryLanguage
                                    TupleQueryResultHandler TupleQueryResult)
           org.eclipse.rdf4j.query.resultio.helpers.BackgroundTupleResult
           org.eclipse.rdf4j.query.resultio.TupleQueryResultFormat
           (org.eclipse.rdf4j.repository.sparql.query SPARQLBooleanQuery SPARQLGraphQuery SPARQLTupleQuery)
           (org.eclipse.rdf4j.rio RDFFormat RDFHandler)
           (java.util.concurrent ThreadPoolExecutor TimeUnit ArrayBlockingQueue)))

(s/def ::core-pool-size pos-int?)
(s/def ::max-pool-size pos-int?)
(s/def ::keep-alive-time-ms integer?)
(s/def ::queue-size pos-int?)

(defmethod ig/pre-init-spec ::cache-thread-pool [_]
  (s/keys :opt-un [::core-pool-size ::max-pool-size ::keep-alive-time-ms ::queue-size]))

;; This threadpool will raise a java.util.concurrent.RejectedExecutionException
;; when there are no free threads to execute tasks and the queue of
;; waiting jobs is full.
(defmethod ig/init-key ::cache-thread-pool [_ {:keys [core-pool-size
                                                      max-pool-size
                                                      keep-alive-time-ms ;; time threads above core-pool-size wait for work before dying.
                                                      queue-size] :as opts}]

  (let [queue (ArrayBlockingQueue. (or queue-size 1))]
    (ThreadPoolExecutor. (or core-pool-size 1)
                         (or max-pool-size 1)
                         (or keep-alive-time-ms 1000)
                         TimeUnit/MILLISECONDS
                         queue)))

(defmethod ig/halt-key! ::cache-thread-pool [k thread-pool]
  (.shutdown thread-pool))

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

(defn fetch-modified-state [repo {:keys [named-graphs default-graphs] :as graphs}]
  (let [values {:graph (set (concat default-graphs named-graphs))}]
    (with-open [conn (repo/->connection repo)]
      (first (sparql/query "drafter/stasher/modified-state.sparql" values conn)))))

(defn generate-drafter-cache-key [query-type _cache query-str ?dataset {raw-repo :raw-repo :as context}]
  {:pre [(or (instance? Dataset ?dataset)
             ;; What does it mean when there is no dataset?
             (nil? ?dataset))]
   :post [(s/valid? ::ck/cache-key %)]}
  (let [graphs (dataset->graphs ?dataset)
        modified-state (fetch-modified-state raw-repo graphs)]
    {:dataset (graphs->edn graphs)
     :query-type query-type
     :query-str query-str
     :modified-times modified-state}))

(defn fetch-cache-parser-and-stream
  "Given a cache and a cache key/query lookup the cached item and
  return a stream and parser on it."

  [cache cache-key]
  (let [cached-file (cache/lookup cache cache-key)
        format (fc/get-format cached-file)

        file-stream-map (if (#{RDFFormat/BINARY TupleQueryResultFormat/BINARY} format)
                          {:charset nil}
                          {:charset "UTF-8"})

        cached-file-parser (fc/get-parser format)]

    (assoc file-stream-map
           :cache-stream (io/input-stream cached-file)
           :cache-parser cached-file-parser)))

(defn- construct-sync-cache-hit
  "Return a BackgroundGraphResult and trigger a thread to iterate over
  the result stream.  BackgroundGraphResult will then marshal the
  events through an iterator-like blocking interface.

  NOTE: there is no need to handle the RDF4j \"dataset\" as cache hits
  will already be on results where the dataset restriction was set."
  [thread-pool cache-key base-uri-str cache]
  (let [start-time (System/currentTimeMillis)
        {:keys [cache-stream cache-parser charset]} (fetch-cache-parser-and-stream cache cache-key)
        bg-graph-result (BackgroundGraphResult. cache-parser cache-stream (when charset (Charset/forName charset)) base-uri-str)]

    ;; execute parse thread on a thread pool.
    (.submit thread-pool ^Runnable (fn []
                                     (try
                                       (.run bg-graph-result)
                                       (dd/histogram! "drafter.stasher.construct_sync.cache_hit"
                                                      (- (System/currentTimeMillis) start-time))
                                       (catch Throwable ex
                                         (log/warn ex "Error reading data from cache")))))

    bg-graph-result))

(defn- tuple-sync-cache-hit
  "Return a BackgroundTupleResult and trigger a thread to iterate over
  the result stream.  BackgroundGraphResult will then marshal the
  events through an iterator-like blocking interface.

  NOTE: there is no need to handle the RDF4j \"dataset\" as cache hits
  will already be on results where the dataset restriction was set."
  [thread-pool cache-key base-uri-str cache]
  (let [start-time (System/currentTimeMillis)
        {:keys [cache-stream cache-parser charset]} (fetch-cache-parser-and-stream cache cache-key)
        bg-tuple-result (BackgroundTupleResult. cache-parser cache-stream)]

    ;; execute parse thread on a thread pool.
    (.submit thread-pool ^Runnable (fn []
                                     (try
                                       (.run bg-tuple-result)
                                       (dd/histogram! "drafter.stasher.tuple_sync.cache_hit"
                                                      (- (System/currentTimeMillis) start-time))
                                       (catch Throwable ex
                                         (log/warn ex "Error reading data from cache")))))
    bg-tuple-result))

(defn boolean-sync-cache-hit [cache-key base-uri-str cache]
  (dd/measure!
   "drafter.stasher.boolean_sync.cache_hit" nil
   (let [{:keys [cache-stream cache-parser charset]} (fetch-cache-parser-and-stream cache cache-key)]
     (.parse cache-parser cache-stream))))

(defn- tuple-time-reporting-query-result
  "Wrap up a tuple query result / handler to record how long it takes."
  [metric-name start-time query-result]
  (reify
    TupleQueryResultHandler
    (endQueryResult [_]
      (.endQueryResult query-result))
    (handleLinks [_ links]
      (.handleLinks query-result links))
    (handleBoolean [_ bool]
      (.handleBoolean query-result bool))
    (handleSolution [_ binding-set]
      (.handleSolution query-result binding-set))
    (startQueryResult [_ binding-names]
      (.startQueryResult query-result binding-names))
    TupleQueryResult
    (getBindingNames [_]
      (.getBindingNames query-result))
    (close [_]
      (.close query-result)
      (dd/histogram! metric-name
                     (- (System/currentTimeMillis) start-time)))
    (hasNext [this]
      (.hasNext ^TupleQueryResult query-result))
    (next [this]
      (.next query-result))))

(defn- construct-time-reporting-query-result
  "Wrap up a construct query result to record how long it takes."
  [metric-name start-time query-result]
  (reify GraphQueryResult
    (getNamespaces [_]
      (.getNamespaces query-result))
    (close [_]
      (.close query-result)
      (dd/histogram! metric-name
                     (- (System/currentTimeMillis) start-time)))
    (hasNext [this]
      (.hasNext query-result))
    (next [this]
      (.next query-result))
    (remove [this]
      (.remove query-result))))

(defn- time-reporting-rdf-handler
  "Wrap up a construct query result to record how long it takes."
  [metric-name start-time rdf-handler]
  (reify RDFHandler
    (startRDF [_]
      (.startRDF rdf-handler))
    (endRDF [_]
      (.endRDF rdf-handler)
      (dd/histogram! metric-name (- (System/currentTimeMillis) start-time)))
    (handleStatement [_ statement]
      (.handleStatement rdf-handler statement))
    (handleComment [_ comment]
      (.handleComment rdf-handler comment))
    (handleNamespace [_ prefix-str uri-str]
      (.handleNamespace rdf-handler prefix-str uri-str))))

(defn construct-sync-cache-miss [httpclient {:keys [query-str] :as cache-key} base-uri-str cache graph-query]
  (let [start-time (System/currentTimeMillis)
        dataset (.getDataset graph-query)
        bg-graph-result (.sendGraphQuery httpclient QueryLanguage/SPARQL
                                         query-str base-uri-str dataset
                                         (.getIncludeInferred graph-query) (.getMaxExecutionTime graph-query)
                                         (.getBindingsArray graph-query))]

    ;; Finally wrap the RDF4j handler we get back in a stashing
    ;; handler that will move the streamed results into the stasher
    ;; cache when it's finished.
    (construct-time-reporting-query-result
     "drafter.stasher.construct_sync.cache_miss"
     start-time
     (fc/stashing-graph-query-result cache cache-key bg-graph-result))))

(defn tuple-sync-cache-miss [httpclient {:keys [query-str] :as cache-key} base-uri-str cache tuple-query]
  (let [start-time (System/currentTimeMillis)
        dataset (.getDataset tuple-query)
        bg-graph-result (.sendTupleQuery httpclient QueryLanguage/SPARQL
                                         query-str base-uri-str dataset
                                         (.getIncludeInferred tuple-query) (.getMaxExecutionTime tuple-query)
                                         (.getBindingsArray tuple-query))]

    ;; Finally wrap the RDF4j handler we get back in a stashing
    ;; handler that will move the streamed results into the stasher
    ;; cache when it's finished.
    (tuple-time-reporting-query-result
     "drafter.stasher.tuple_sync.cache_miss"
     start-time
     (fc/stashing-tuple-query-result :sync cache cache-key bg-graph-result))))

(defn boolean-sync-cache-miss [httpclient {:keys [query-str] :as cache-key} base-uri-str cache boolean-query]
  (dd/measure!
   "drafter.stasher.boolean_sync.cache_miss" nil
   (let [dataset (.getDataset boolean-query)
         boolean-result (.sendBooleanQuery httpclient QueryLanguage/SPARQL
                                           query-str base-uri-str dataset
                                           (.getIncludeInferred boolean-query) (.getMaxExecutionTime boolean-query)
                                           (.getBindingsArray boolean-query))]

     ;; Finally wrap the RDF4j handler we get back in a stashing
     ;; handler that will move the streamed results into the stasher
     ;; cache when it's finished.
     (fc/stashing-boolean-query-result cache cache-key boolean-result))))

(defn- construct-async-cache-hit
  [query-str rdf-handler base-uri-str cache]
  (let [{:keys [cache-stream cache-parser charset]} (fetch-cache-parser-and-stream cache query-str)]
             (doto cache-parser
               (.setRDFHandler rdf-handler)
               (.parse cache-stream base-uri-str))))

(defn- tuple-async-cache-hit
  [query-str tuple-handler base-uri-str cache]
  (let [{:keys [cache-stream cache-parser charset]} (fetch-cache-parser-and-stream cache query-str)]
             (doto cache-parser
               (.setQueryResultHandler tuple-handler)
               (.parse cache-stream))))

(defn stashing-construct-query
  "Construct a graph query that checks the stash before evaluating"
  [httpclient cache query-str base-uri-str {:keys [cache-key-generator thread-pool] :as opts}]
  (let [cache cache #_(fc/file-cache-factory {})] ;; TODO fix this up to use atom/cache pattern
    (proxy [SPARQLGraphQuery] [httpclient base-uri-str query-str]
      (evaluate
        ;; sync results
        ([]
         (let [dataset (.getDataset this)
               cache-key (cache-key-generator :graph cache query-str dataset opts)]
           (if (cache/has? cache cache-key)
             (construct-sync-cache-hit thread-pool cache-key base-uri-str cache)

             ;; else send query (and simultaneously stream it to file that gets put in the cache)
             (construct-sync-cache-miss httpclient cache-key base-uri-str cache this))))

        ;; async results
        ([rdf-handler]
         (let [dataset (.getDataset this)
               cache-key (cache-key-generator :graph cache query-str dataset opts)]
           (if (cache/has? cache cache-key)
             (let [rdf-handler (time-reporting-rdf-handler "drafter.stasher.construct_async.cache_hit"
                                                           (System/currentTimeMillis)
                                                           rdf-handler)]
               (construct-async-cache-hit cache-key rdf-handler base-uri-str cache))

             ;; else
             (let [dataset (.getDataset this)
                   stashing-rdf-handler (fc/stashing-rdf-handler cache cache-key rdf-handler)
                   stashing-rdf-handler (time-reporting-rdf-handler "drafter.stasher.construct_async.cache_miss"
                                                                              (System/currentTimeMillis)
                                                                              stashing-rdf-handler)]
               (.sendGraphQuery httpclient QueryLanguage/SPARQL
                                query-str base-uri-str dataset
                                (.getIncludeInferred this) (.getMaxExecutionTime this)
                                stashing-rdf-handler (.getBindingsArray this))))))))))

(defn stashing-select-query
  "Construct a tuple query that checks the stash before evaluating"
  [httpclient cache query-str base-uri-str {:keys [cache-key-generator thread-pool] :as opts}]
  (let [cache cache #_(fc/file-cache-factory {})] ;; TODO fix this up to use atom/cache pattern
    (proxy [SPARQLTupleQuery] [httpclient base-uri-str query-str]
      (evaluate
        ;; sync results
        ([]
         (let [dataset (.getDataset this)
               cache-key (cache-key-generator :tuple cache query-str dataset opts)]
           (if (cache/has? cache cache-key)
             (tuple-sync-cache-hit thread-pool cache-key base-uri-str cache)

             ;; else send query (and simultaneously stream it to file that gets put in the cache)
             (tuple-sync-cache-miss httpclient cache-key base-uri-str cache this))))

        ;; TODO TODO TODO
        ;; async results
        ([tuple-handler]
         (let [dataset (.getDataset this)
               cache-key (cache-key-generator :tuple cache query-str dataset opts)]
           (if (cache/has? cache cache-key)
             (let [tuple-handler (tuple-time-reporting-query-result "drafter.stasher.tuple_async.cache_hit"
                                                                    (System/currentTimeMillis)
                                                                    tuple-handler)]
               (tuple-async-cache-hit cache-key tuple-handler base-uri-str cache))

             ;; else
             (let [stashing-tuple-handler (tuple-time-reporting-query-result
                                           "drafter.stasher.tuple_async.cache_miss"
                                           (System/currentTimeMillis)
                                           (fc/stashing-tuple-query-result :async cache cache-key tuple-handler))
                   dataset (.getDataset this)]
               (.sendTupleQuery httpclient QueryLanguage/SPARQL
                                query-str base-uri-str dataset
                                (.getIncludeInferred this) (.getMaxExecutionTime this)
                                stashing-tuple-handler (.getBindingsArray this))))))))))

(defn stashing-boolean-query
  "Construct a boolean query that checks the stash before evaluating"
  [httpclient cache query-str base-uri-str {:keys [cache-key-generator thread-pool] :as opts}]
  (let [cache cache #_(fc/file-cache-factory {})] ;; TODO fix this up to use atom/cache pattern
    (proxy [SPARQLBooleanQuery] [httpclient query-str base-uri-str]
      (evaluate []
        ;; sync results only
        (let [dataset (.getDataset this)
              cache-key (cache-key-generator :boolean cache query-str dataset opts)]
          (if (cache/has? cache cache-key)
            (boolean-sync-cache-hit cache-key base-uri-str cache)

            ;; else send query (and simultaneously stream it to file that gets put in the cache)
            (boolean-sync-cache-miss httpclient cache-key base-uri-str cache this)))

        ;; NOTE unlike for the other two query types there is no
        ;; async/handler interface for boolean queries defined in
        ;; RDF4j.  So there is no evaluate body with a handler arg as
        ;; with other query types.
        ))))



(defn stasher-update-query
  "Construct a stasher update query to expire cache etc"
  [httpclient cache query-str base-uri-str]
  )

(defn- stasher-connection [repo httpclient cache {:keys [quad-mode base-uri] :or {quad-mode false} :as opts}]
  (proxy [DrafterSPARQLConnection] [repo httpclient quad-mode]

;    #_(commit) ;; catch potential cache expirey
;    #_(prepareUpdate [_ query-str base-uri-str]);; catch
    
    (prepareTupleQuery [_ query-str base-uri-str]
      (stashing-select-query httpclient cache query-str (or base-uri-str base-uri) opts))
    
    (prepareGraphQuery [_ query-str base-uri-str]
      (stashing-construct-query httpclient cache query-str (or base-uri-str base-uri) opts))

    (prepareBooleanQuery [_ query-str base-uri-str]
      (stashing-boolean-query httpclient cache query-str (or base-uri-str base-uri) opts))))

(defn stasher-repo
  "Builds a stasher RDF repository, that implements the standard RDF4j
  repository interface but caches query results to disk and
  transparently returns cached results if they're in the cache."
  [{:keys [sparql-query-endpoint sparql-update-endpoint report-deltas cache] :as opts}]
  (let [query-endpoint (str sparql-query-endpoint)
        update-endpoint (str sparql-update-endpoint)
        cache (or cache (fc/file-cache-factory {}))
        deltas (boolean (or report-deltas true))

        ;; construct a second hidden raw-repo for performing uncached
        ;; queries on, e.g. draftset modified times.
        raw-repo (doto (DrafterSPARQLRepository. query-endpoint update-endpoint)
                   (.initialize))
        updated-opts (assoc opts
                            :raw-repo raw-repo
                            :cache-key-generator (or (:cache-key-generator opts)
                                                     generate-drafter-cache-key)
                            :base-uri (or (:base-uri opts)
                                          "http://publishmydata.com/id/"))]

    (repo/notifying-repo (proxy [DrafterSPARQLRepository] [query-endpoint update-endpoint]
                           (getConnection []
                             (stasher-connection this (.createHTTPClient this) cache updated-opts))) deltas)))

(defmethod ig/init-key :drafter.stasher/repo [_ opts]
  (stasher-repo opts))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Specs

(s/def ::sparql-query-endpoint uri?)
(s/def ::sparql-update-endpoint uri?)
(s/def ::thread-pool #(instance? java.util.concurrent.ThreadPoolExecutor %))

(defmethod ig/pre-init-spec :drafter.stasher/repo [_]
  (s/keys :req-un [::sparql-query-endpoint ::sparql-update-endpoint ::thread-pool]))
