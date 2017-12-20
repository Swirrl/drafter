(ns drafter.stasher
  (:require [clojure.core.cache :as cache]
            [clojure.java.io :as io]
            [clojure.spec.alpha :as s]
            [drafter.stasher.filecache :as fc]
            [grafter.rdf4j.repository :as repo]
            [grafter.rdf4j.sparql :as sparql]
            [integrant.core :as ig]
            [clojure.tools.logging :as log])
  (:import (drafter.rdf DrafterSPARQLConnection DrafterSPARQLRepository)
           java.nio.charset.Charset
           org.eclipse.rdf4j.query.impl.BackgroundGraphResult
           org.eclipse.rdf4j.query.QueryLanguage
           org.eclipse.rdf4j.query.resultio.helpers.BackgroundTupleResult
           org.eclipse.rdf4j.query.resultio.TupleQueryResultFormat
           (org.eclipse.rdf4j.repository.sparql.query SPARQLGraphQuery SPARQLTupleQuery)
           org.eclipse.rdf4j.rio.RDFFormat
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

(defn dataset->edn
  "Convert a Dataset object into a clojure value with consistent print
  order, so it's suitable for hashing."
  [dataset]
  (when dataset
    {:default-graphs (sort (map str (.getDefaultGraphs dataset)))
     :named-graphs (sort (map str (.getNamedGraphs dataset)))}))

(defn fetch-modified-state [repo dataset]
  (let [values {:graph (set (when dataset (concat (.getDefaultGraphs dataset)
                                                  (.getNamedGraphs dataset))))}]
    (with-open [conn (repo/->connection repo)]
      (first (doall (sparql/query "drafter/stasher/modified-state.sparql" values conn))))))

(defn generate-drafter-cache-key [query-type _cache query-str dataset {raw-repo :raw-repo :as context}]
  (let [modified-state (fetch-modified-state raw-repo dataset)
        dataset-value (dataset->edn dataset)]

    (let [k {:dataset dataset-value
             :query-type query-type  ;; This is the only field required by the file-cache in the cache-key
             :query-str query-str
             :modified-times modified-state}]
      k)))

(defn stashing->boolean-query
  "Construct a boolean query that checks the stash before evaluating"
  [httpclient cache query-str base-uri-str])

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
  (let [{:keys [cache-stream cache-parser charset]} (fetch-cache-parser-and-stream cache cache-key)       
        bg-graph-result (BackgroundGraphResult. cache-parser cache-stream (when charset (Charset/forName charset)) base-uri-str)]

    ;; execute parse thread on a thread pool.
    (.submit thread-pool ^Runnable (fn []
                                     (try
                                       (.run bg-graph-result)
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
  (let [{:keys [cache-stream cache-parser charset]} (fetch-cache-parser-and-stream cache cache-key)       
        bg-tuple-result (BackgroundTupleResult. cache-parser cache-stream)]

    ;; execute parse thread on a thread pool.
    (.submit thread-pool ^Runnable (fn []
                                     (try
                                       (.run bg-tuple-result)
                                       (catch Throwable ex
                                         (log/warn ex "Error reading data from cache"))))) 
    bg-tuple-result))

(defn construct-sync-cache-miss [httpclient {:keys [query-str] :as cache-key} base-uri-str cache graph-query]
  (let [dataset (.getDataset graph-query)
        bg-graph-result (.sendGraphQuery httpclient QueryLanguage/SPARQL
                                         query-str base-uri-str dataset
                                         (.getIncludeInferred graph-query) (.getMaxExecutionTime graph-query)
                                         (.getBindingsArray graph-query))]

    ;; Finally wrap the RDF4j handler we get back in a stashing
    ;; handler that will move the streamed results into the stasher
    ;; cache when it's finished.
    (fc/stashing-graph-query-result cache cache-key bg-graph-result)))

(defn tuple-sync-cache-miss [httpclient {:keys [query-str] :as cache-key} base-uri-str cache graph-query]
  (let [dataset (.getDataset graph-query)
        bg-graph-result (.sendTupleQuery httpclient QueryLanguage/SPARQL
                                         query-str base-uri-str dataset
                                         (.getIncludeInferred graph-query) (.getMaxExecutionTime graph-query)
                                         (.getBindingsArray graph-query))]

    ;; Finally wrap the RDF4j handler we get back in a stashing
    ;; handler that will move the streamed results into the stasher
    ;; cache when it's finished.
    (fc/stashing-tuple-query-result cache cache-key bg-graph-result)))

(defn- construct-async-cache-hit
  [query-str rdf-handler base-uri-str cache]
  (let [{:keys [cache-stream cache-parser charset]} (fetch-cache-parser-and-stream cache query-str)]
             (doto cache-parser
               (.setRDFHandler rdf-handler)
               (.parse cache-stream base-uri-str))))

(defn stashing->construct-query
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
             (construct-async-cache-hit cache-key rdf-handler base-uri-str cache)
             
             ;; else
             (let [stashing-rdf-handler (fc/stashing-rdf-handler cache cache-key rdf-handler)
                   dataset (.getDataset this)]
               (.sendGraphQuery httpclient QueryLanguage/SPARQL
                                query-str base-uri-str dataset
                                (.getIncludeInferred this) (.getMaxExecutionTime this)
                                stashing-rdf-handler (.getBindingsArray this))))))))))
;;;;; TODO TODO TODO
(defn stashing->select-query
  "Construct a tuple query that checks the stash before evaluating"
  [httpclient cache query-str base-uri-str {:keys [cache-key-generator thread-pool] :as opts}]
  (let [cache cache #_(fc/file-cache-factory {})] ;; TODO fix this up to use atom/cache pattern
    (proxy [SPARQLTupleQuery] [httpclient base-uri-str query-str]
      (evaluate
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
             (construct-async-cache-hit cache-key tuple-handler base-uri-str cache)
             
             ;; else
             (let [stashing-tuple-handler (fc/stashing-tuple-query-result cache cache-key tuple-handler)
                   dataset (.getDataset this)]
               (.sendTupleQuery httpclient QueryLanguage/SPARQL
                                query-str base-uri-str dataset
                                (.getIncludeInferred this) (.getMaxExecutionTime this)
                                stashing-tuple-handler (.getBindingsArray this))))))))))



(defn stasher-update-query
  "Construct a stasher update query to expire cache etc"
  [httpclient cache query-str base-uri-str]
  )

(defn- stasher-connection [repo httpclient cache {:keys [quad-mode base-uri] :or {quad-mode false} :as opts}]
  (proxy [DrafterSPARQLConnection] [repo httpclient quad-mode]

;    #_(commit) ;; catch potential cache expirey
;    #_(prepareUpdate [_ query-str base-uri-str]);; catch
    
    (prepareTupleQuery [_ query-str base-uri-str]
      (stashing->select-query httpclient cache query-str (or base-uri-str base-uri) opts))
    
    (prepareGraphQuery [_ query-str base-uri-str]
      (stashing->construct-query httpclient cache query-str (or base-uri-str base-uri) opts))

    #_(prepareBooleanQuery [_ query-str base-uri-str])
    )

  
    )

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


