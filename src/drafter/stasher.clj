(ns drafter.stasher
  (:require [clojure.core.cache :as cache]
            [clojure.java.io :as io]
            [grafter.rdf4j.repository :as repo]
            [grafter.rdf :as rdf]
            [grafter.rdf4j.io :as gio]
            [grafter.rdf.protocols :as pr]
            [me.raynes.fs :as fs]
            [grafter.rdf4j.formats :as fmt]
            [drafter.stasher.filecache :as fc]
            [clojure.spec.alpha :as s]
            [clojure.spec.gen.alpha :as g]
            [integrant.core :as ig]
            [clojure.tools.logging :as log]
            [grafter.rdf4j.sparql :as sparql])
  (:import org.eclipse.rdf4j.repository.event.RepositoryConnectionListener
           java.net.URI
           (drafter.rdf DrafterSPARQLConnection DrafterSPARQLRepository DrafterSparqlSession)
           (org.eclipse.rdf4j.repository Repository RepositoryConnection)
           (org.eclipse.rdf4j.query QueryLanguage GraphQueryResult TupleQueryResult)
           (org.eclipse.rdf4j.repository.event.base NotifyingRepositoryWrapper NotifyingRepositoryConnectionWrapper)
           (org.eclipse.rdf4j.repository.sparql.query SPARQLBooleanQuery SPARQLGraphQuery SPARQLTupleQuery SPARQLUpdate)
           (org.eclipse.rdf4j.rio RDFHandler)
           (org.eclipse.rdf4j.model.impl URIImpl)
           (org.eclipse.rdf4j.query.impl SimpleDataset)
           (org.eclipse.rdf4j.query.impl BackgroundGraphResult)
           java.io.File
           (java.security DigestOutputStream DigestInputStream MessageDigest)
           org.apache.commons.codec.binary.Hex))


(defn dataset->edn
  "Convert a Dataset object into a clojure value with consistent print
  order, so it's suitable for hashing."
  [dataset]
  (when dataset
    {:default-graphs (sort (map str (.getDefaultGraphs dataset)))
     :named-graphs (sort (map str (.getNamedGraphs dataset)))}))


(defn fetch-modified-state [repo {:keys [default-graphs named-graphs]}]
  (let [values {:graph (set (concat default-graphs
                                    named-graphs))}]
    (with-open [conn (repo/->connection repo)]
      (first (doall (sparql/query "drafter/stasher/modified-state.sparql" values conn))))))

(defn build-composite-cache-key [cache query-str dataset {repo :raw-repo :as context}]
  (let [dataset (dataset->edn dataset)
        modified-state (fetch-modified-state repo dataset)]
    {:dataset dataset
     :query query-str
     :modified-times modified-state} ))

(defn stashing->boolean-query
  "Construct a boolean query that checks the stash before evaluating"
  [httpclient cache query-str base-uri-str])

(defn stashing->tuple-query
  "Construct a tuple query that checks the stash before evaluating"
  [httpclient cache query-str base-uri-str])

(defn fetch-cache-parser-and-stream
  "Given a cache and a cache key/query lookup the cached item and
  return a stream and parser on it."
  [cache cache-key]
  (let [cached-file-stream (io/input-stream (cache/lookup cache cache-key))
        cached-file-parser (-> cache
                               fc/backend-rdf-format
                               fmt/->rdf-format
                               fmt/format->parser)]

    {:cache-stream cached-file-stream
     :cache-parser cached-file-parser
     :charset nil ;; as we're using binary file format for cache ;; TODO move this into file-cache object / config
     }))

(defn- construct-sync-cache-hit
  "Return a BackgroundGraphResult and trigger a thread to iterate over
  the result stream.  BackgroundGraphResult will then marshal the
  events through an iterator-like blocking interface.

  NOTE: there is no need to handle the RDF4j \"dataset\" as cache hits
  will already be on results where the dataset restriction was set."
  [cache-key base-uri-str cache]
  (let [{:keys [cache-stream cache-parser charset]} (fetch-cache-parser-and-stream cache cache-key)       
        bg-graph-result (BackgroundGraphResult. cache-parser cache-stream charset base-uri-str)]

    ;; execute parse thread on a thread pool.
    (.submit clojure.lang.Agent/soloExecutor bg-graph-result) 
    bg-graph-result))

(defn construct-sync-cache-miss [httpclient query-str base-uri-str cache graph-query]
  (let [dataset (.getDataset graph-query)
        bg-graph-result (.sendGraphQuery httpclient QueryLanguage/SPARQL
                                         query-str base-uri-str dataset
                                         (.getIncludeInferred graph-query) (.getMaxExecutionTime graph-query)
                                         (.getBindingsArray graph-query))]

    ;; Finally wrap the RDF4j handler we get back in a stashing
    ;; handler that will move the streamed results into the stasher
    ;; cache when it's finished.
    (fc/stashing-graph-query-result cache query-str bg-graph-result)))

(defn- construct-async-cache-hit
  [query-str rdf-handler base-uri-str cache]
  (let [{:keys [cache-stream cache-parser charset]} (fetch-cache-parser-and-stream cache query-str)]
             (doto cache-parser
               (.setRDFHandler rdf-handler)
               (.parse cache-stream base-uri-str))))

(defn stashing->construct-query
  "Construct a tuple query that checks the stash before evaluating"
  [httpclient cache query-str base-uri-str {:keys [cache-key-generator] :as opts}]
  (let [cache (fc/file-cache-factory {})] ;; TODO fix this up to use atom/cache pattern
    (proxy [SPARQLGraphQuery] [httpclient query-str base-uri-str]
      (evaluate
        ;; sync results
        ([]
         (let [dataset (.getDataset this)
               cache-key (cache-key-generator cache query-str dataset opts)]
           (if (cache/has? cache cache-key) ;; TODO build composite keys
             (construct-sync-cache-hit cache-key base-uri-str cache)
             
             ;; else send query (and simultaneously stream it to file that gets put in the cache)
             (construct-sync-cache-miss httpclient query-str base-uri-str cache this))))

        ;; async results
        ([rdf-handler]
         (if (cache/has? cache query-str) ;; TODO build composite keys
           (construct-async-cache-hit query-str rdf-handler base-uri-str cache)
           
           ;; else
           (let [stashing-rdf-handler (fc/stashing-rdf-handler cache query-str rdf-handler)
                 dataset (.getDataset this)]
             (.sendGraphQuery httpclient QueryLanguage/SPARQL
                              query-str base-uri-str dataset
                              (.getIncludeInferred this) (.getMaxExecutionTime this)
                              stashing-rdf-handler (.getBindingsArray this)))))))))

(defn stasher-update-query
  "Construct a stasher update query to expire cache etc"
  [httpclient cache query-str base-uri-str]
  )

(defn- stasher-connection [repo httpclient cache {:keys [quad-mode] :or {quad-mode false} :as opts}]
  (proxy [DrafterSPARQLConnection] [repo httpclient quad-mode]

    #_(commit) ;; catch potential cache expirey
    #_(prepareUpdate [_ query-str base-uri-str]);; catch
    
    #_(prepareTupleQuery [_ query-str base-uri-str])
    (prepareGraphQuery [_ query-str base-uri-str]
      (stashing->construct-query httpclient cache query-str base-uri-str opts))
    
    #_(prepareBooleanQuery [_ query-str base-uri-str])
    ))

(defn stasher-repo
  "Builds a stasher RDF repository, that implements the standard RDF4j
  repository interface but caches query results to disk and
  transparently returns cached results if they're in the cache.
  
  "
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
                                                     build-composite-cache-key))]

    (repo/notifying-repo (proxy [DrafterSPARQLRepository] [query-endpoint update-endpoint]
                           (getConnection []
                             (stasher-connection this (.createHTTPClient this) cache updated-opts))) deltas)))

(defmethod ig/init-key :drafter.stasher/repo [_ opts]
  (stasher-repo opts))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Specs

(s/def ::sparql-query-endpoint uri?)
(s/def ::sparql-update-endpoint uri?)

(defmethod ig/pre-init-spec :drafter.stasher/repo [_]
  (s/keys :req-un [::sparql-query-endpoint ::sparql-update-endpoint]))


