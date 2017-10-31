(ns drafter.stasher
  (:require [clojure.core.cache :as cache]
            [clojure.java.io :as io]
            [grafter.rdf.repository :as repo]
            [grafter.rdf :as rdf]
            [grafter.rdf4j.io :as gio]
            [grafter.rdf.protocols :as pr]
            [me.raynes.fs :as fs]
            [grafter.rdf.formats :as fmt])
  (:import org.eclipse.rdf4j.repository.event.RepositoryConnectionListener
           java.net.URI
           (drafter.rdf DrafterSPARQLConnection DrafterSPARQLRepository DrafterSparqlSession)
           (org.eclipse.rdf4j.repository Repository RepositoryConnection)
           (org.eclipse.rdf4j.query QueryLanguage)
           (org.eclipse.rdf4j.repository.event.base NotifyingRepositoryWrapper NotifyingRepositoryConnectionWrapper)
           (org.eclipse.rdf4j.repository.sparql.query SPARQLBooleanQuery SPARQLGraphQuery SPARQLTupleQuery SPARQLUpdate)
           (org.eclipse.rdf4j.rio RDFHandler)
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

(defn md5-sum [cache-key]
  (let [cache-key-str (pr-str cache-key)
        md (MessageDigest/getInstance "MD5")]
    (Hex/encodeHexString (.digest md (.getBytes cache-key-str)))))

(defn cache-key->file-path [cache-key fmt-extension]
  (let [hex-str (md5-sum cache-key)
        sub-dirs (->> hex-str
                      (partition 2)
                      (take 2)
                      (mapv (partial apply str)))]
    (apply io/file (conj sub-dirs (str hex-str "." (name fmt-extension))))))

(defn- move-tmpfile-to-cache [cache cache-key temp-file]
  (let [cache-key-fname (cache-key->file-path cache-key)
        new-path (io/file (:dir cache) cache-key-fname)]
    (io/make-parents cache-key-fname)
    (fs/rename temp-file new-path)))

(defn stash-rdf-handler
  ""
  [cache cache-key inner-rdf-handler]
  (let [rdf-format (:backend-rdf-format cache :brf)
        temp-file (.createTempFile "stasher" (str "tmp." (name rdf-format)) (io/file (:dir cache) "tmp"))
        make-stream (fmt/select-output-coercer rdf-format)
        stream (make-stream :buffer 8192)
        cache-file-writer (gio/rdf-writer stream :format rdf-format)]

    (reify RDFHandler
      (startRDF [this]
        (.startRDF cache-file-writer)
        (.startRDF inner-rdf-handler))
      (endRDF [this]
        (.endRDF cache-file-writer)
        (.endRDF inner-rdf-handler)
        (.close stream)
        (move-tmpfile-to-cache cache temp-file))
      (handleStatement [this statement]
        (.handleStatement cache-file-writer statement)
        (.handleStatement inner-rdf-handler statement))
      (handleComment [this comment]
        (.handleComment cache-file-writer comment)
        (.handleComment inner-rdf-handler comment))
      (handleNamespace [this prefix-str uri-str]
        (.handleNamespace cache-file-writer prefix-str uri-str)
        (.handleNamespace inner-rdf-handler prefix-str uri-str)))))


(defn stashing->construct-query
  "Construct a tuple query that checks the stash before evaluating"
  [httpclient cache query-str base-uri-str]
  (proxy [SPARQLGraphQuery] [httpclient query-str base-uri-str]
    (evaluate
      ([]
       ;; TODO will need to support these also...
       (.sendGraphQuery httpclient QueryLanguage/SPARQL query-str base-uri-str dataset ;; TODO handle dataset
                        (.getIncludeInferred this) (.getMaxExecutionTime this) (.getBindingsArray this)))
      ([rdf-handler]
       (if (cache/has? cache query-str)
         (cache/lookup cache query-str)
         (.sendGraphQuery httpclient QueryLanguage/SPARQL query-str base-uri-str dataset ;; TODO handle dataset
                          (.getIncludeInferred this) (.getMaxExecutionTime this) rdf-handler (.getBindingsArray this)))))))

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
  ([query-endpoint update-endpoint]
   (stasher-repo query-endpoint update-endpoint (file-cache-factory todo-replace-me-with-init-base-cache)))
  ([query-endpoint update-endpoint cache]
   (stasher-repo query-endpoint update-endpoint (file-cache-factory todo-replace-me-with-init-base-cache) {}))
  ([query-endpoint update-endpoint cache opts]
   (let [deltas (boolean (:report-deltas opts true))]
     (repo/notifying-repo (proxy [DrafterSPARQLRepository] [query-endpoint update-endpoint]
                            (getConnection []
                              (stasher-connection this (.createHTTPClient this) cache opts))) deltas))))





(let [at (atom [])
      listener (reify RepositoryConnectionListener ;; todo can extract for in memory change detection
                 (add [this conn sub pred obj graphs]
                   #_(println "adding")
                   (swap! at conj [:add sub pred obj graphs]))
                 (begin [this conn]
                   #_(println "begin")
                   (swap! at conj [:begin]))
                 (close [this conn]
                   #_(println "close")
                   (swap! at conj [:close]))
                 (commit [this conn]
                   #_(println "commit")
                   (swap! at conj [:commit]))
                 (execute [this conn ql updt-str base-uri operation]
                   #_(println "execute")
                   (swap! at conj [:execute ql updt-str base-uri operation])))
      
      repo (doto (stasher-repo "http://localhost:5820/drafter-test-db/query" "http://localhost:5820/drafter-test-db/update")
             (.addRepositoryConnectionListener listener))]

  (with-open [conn (repo/->connection repo)]
    (rdf/add conn [(pr/->Quad (URI. "http://foo") (URI. "http://foo") (URI. "http://foo") (URI. "http://foo"))])
    (println (doall (repo/query conn "construct { ?s ?p ?o } where { ?s ?p ?o } limit 10")))

    (pr/update! conn "drop all"))
  
  @at)








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
              (startRDF [this]
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
