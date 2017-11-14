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
            [clojure.tools.logging :as log])
  (:import org.eclipse.rdf4j.repository.event.RepositoryConnectionListener
           java.net.URI
           (drafter.rdf DrafterSPARQLConnection DrafterSPARQLRepository DrafterSparqlSession)
           (org.eclipse.rdf4j.repository Repository RepositoryConnection)
           (org.eclipse.rdf4j.query QueryLanguage GraphQueryResult TupleQueryResult)
           (org.eclipse.rdf4j.repository.event.base NotifyingRepositoryWrapper NotifyingRepositoryConnectionWrapper)
           (org.eclipse.rdf4j.repository.sparql.query SPARQLBooleanQuery SPARQLGraphQuery SPARQLTupleQuery SPARQLUpdate)
           (org.eclipse.rdf4j.rio RDFHandler)
           (org.eclipse.rdf4j.query.impl BackgroundGraphResult)
           java.io.File
           (java.security DigestOutputStream DigestInputStream MessageDigest)
           org.apache.commons.codec.binary.Hex))


(defn stashing->boolean-query
  "Construct a boolean query that checks the stash before evaluating"
  [httpclient cache query-str base-uri-str])

(defn stashing->tuple-query
  "Construct a tuple query that checks the stash before evaluating"
  [httpclient cache query-str base-uri-str])

(def dataset nil) ;; TODO

(defn- construct-sync-cache-hit [cache query-str base-uri-str httpclient this]
  ;; cache hit!
  (let [cached-file-stream (io/input-stream (cache/lookup cache query-str))
        cached-file-parser (-> cache
                               fc/backend-rdf-format
                               fmt/->rdf-format
                               fmt/format->parser)
        charset nil ;; as we're using binary file format for cache ;; TODO move this into file-cache object / config
        bg-graph-result (BackgroundGraphResult. cached-file-parser cached-file-stream charset base-uri-str)]

    ;; execute parse thread on a thread pool.
    (.submit clojure.lang.Agent/soloExecutor bg-graph-result) 
    bg-graph-result))

(defn construct-sync-cache-miss [httpclient query-str base-uri-str this cache]
  (let [bg-graph-result (.sendGraphQuery httpclient QueryLanguage/SPARQL query-str base-uri-str dataset ;; TODO handle dataset
                                         (.getIncludeInferred this) (.getMaxExecutionTime this) (.getBindingsArray this))]
    (fc/stashing-graph-query-result cache query-str bg-graph-result)))

(defn stashing->construct-query
  "Construct a tuple query that checks the stash before evaluating"
  [httpclient cache query-str base-uri-str]
  (let [cache (fc/file-cache-factory {})] ;; TODO fix this up to use atom/cache pattern
    (proxy [SPARQLGraphQuery] [httpclient query-str base-uri-str]
      (evaluate
        ;; sync results
        ([]
         (if (cache/has? cache query-str) ;; TODO build composite keys
           (construct-sync-cache-hit cache query-str base-uri-str httpclient this)
           
           ;; else send query (and simultaneously stream it to file that gets put in the cache)
           (construct-sync-cache-miss httpclient query-str base-uri-str this cache)))

        ;; async results
        ([rdf-handler]
         (if (cache/has? cache query-str) ;; TODO build composite keys
           (let [cached-file-stream (io/input-stream (cache/lookup cache query-str))
                 cached-file-parser (-> cache
                                        fc/backend-rdf-format
                                        fmt/->rdf-format
                                        fmt/format->parser)
                 ;; as we're using binary file format for
                 ;; cache TODO move this into file-cache
                 ;; object / config
                 charset nil]

             (doto cached-file-parser
               (.setRDFHandler rdf-handler)
               (.parse cached-file-stream base-uri-str)))
           
           ;; else
           (let [stashing-rdf-handler (fc/stashing-rdf-handler cache query-str rdf-handler)]
             (.sendGraphQuery httpclient QueryLanguage/SPARQL query-str base-uri-str dataset ;; TODO handle dataset
                              (.getIncludeInferred this) (.getMaxExecutionTime this) stashing-rdf-handler (.getBindingsArray this)))))))))

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
      (stashing->construct-query httpclient cache query-str base-uri-str))
    
    #_(prepareBooleanQuery [_ query-str base-uri-str])
    ))

(defn stasher-repo
  [{:keys [sparql-query-endpoint sparql-update-endpoint report-deltas cache] :as opts}]
  (let [query-endpoint (str sparql-query-endpoint)
        update-endpoint (str sparql-update-endpoint)
        cache (or cache (fc/file-cache-factory {}))
        deltas (boolean (or report-deltas true))]
    (repo/notifying-repo (proxy [DrafterSPARQLRepository] [query-endpoint update-endpoint]
                           (getConnection []
                             (stasher-connection this (.createHTTPClient this) cache opts))) deltas)))

(defmethod ig/init-key :drafter.stasher/repo [_ opts]
  (stasher-repo opts))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Specs

(s/def ::sparql-query-endpoint uri?)
(s/def ::sparql-update-endpoint uri?)

(defmethod ig/pre-init-spec :drafter.stasher/repo [_]
  (s/keys :req-un [::sparql-query-endpoint ::sparql-update-endpoint]))



;; TODO it would be great to include something like this in grafter,
;; but it will need to proxy by default.  Might be a good candidate
;; for a macro.

(comment (defn comp-rdf-handler
           "Compose RDFHandlers together like with comp the argument order is
  reversed.  i.e.  Handlers are applied right to left as with function
  composition."
           ([rdf-handler]
            rdf-handler)
           ([^RDFHandler rdf-handler-b ^RDFHandler rdf-handler-a]
            (reify RDFHandler
              #_(startRDF [this]
                (.startRDF rdf-handler-a)
                (.startRDF rdf-handler-b))
              (endRDF [this]
                (.endRDF rdf-handler-a)
                (.endRDF rdf-handler-b))
              (handleStatement [this statement]
                (.handleStatement rdf-handler-a statement)
                (.handleStatement rdf-handler-b statement))
              (handleComment [this comment]
                (.handleComment rdf-handler-a comment)
                (.handleComment rdf-handler-b comment))
              (handleNamespace [this prefix-str uri-str]
                (.handleNamespace rdf-handler-a prefix-str uri-str)
                (.handleNamespace rdf-handler-b prefix-str uri-str))))
           ([rdf-handler-b rdf-handler-a & rdf-handlers]
            (reduce comp-rdf-handler (list* rdf-handler-b rdf-handler-a rdf-handlers)))))


;; We need to delegate the whole interface just to override the one
;; getConnection method. It's a little easier to do things this way
;; than use RDF4j's AbstractRepository as inheritance is a little
;; harder in Clojure.
#_(defrecord StasherRepo [repo cache]
  Repository
  (getConnection [this]
    (->StasherConnection (.getConnection repo) cache))

  (getDataDir [this]
    (:dir cache))

  (getValueFactory [this]
    (.getValueFactory this))

  (initialize [this]
    (.initialize this))

  (isInitialized [this]
    (.isInitialized this))

  (isWritable [this]
    (.isWritable this))

  (setDataDir [this dir]
    (.setDataDir this dir))

  (shutDown [this]
    (.shutDown this)))

#_(defn stasher-repo [query-endpoint update-endpoint cache opts]
  (let [sparql-repo (repo/sparql-repo query-endpoint update-endpoint)]
    (->StasherRepo (repo/notifying-repo sparql-repo (:report-deltas opts)) cache))
  )
