(ns drafter.backend.protocols
  (:require [grafter.rdf4j.repository :as repo]
            [grafter.rdf.protocols :as proto])
  (:import java.net.URI))

(defprotocol SparqlExecutor
  (prepare-query [this sparql-string]))

(defprotocol ToRepository
  (->sesame-repo [this]
    "Gets the sesame repository for this backend"))

(defn stop-backend [backend]
  (repo/shutdown (->sesame-repo backend)))

(defn ->repo-connection
  "Opens a connection to the underlying Sesame repository for the
  given backend."
  [backend]
  (repo/->connection (->sesame-repo backend)))


(def itriple-readable-delegate
  {:to-statements (fn [this options]
                    (proto/to-statements (->sesame-repo this) options))})

(def isparqlable-delegate
  {:query-dataset (fn [this sparql-string model]
                    (proto/query-dataset (->sesame-repo this) sparql-string model))})

(def isparql-updateable-delegate
  {:update! (fn [this sparql-string]
              (proto/update! (->sesame-repo this) sparql-string))})

(def to-connection-delegate
  {:->connection (fn [this]
                   (repo/->connection (->sesame-repo this)))})

(defn- add-delegate
  ([this triples] (proto/add (->sesame-repo this) triples))
  ([this graph triples] (proto/add (->sesame-repo this) graph triples))
  ([this graph format triple-stream] (proto/add (->sesame-repo this) graph format triple-stream))
  ([this graph base-uri format triple-stream] (proto/add (->sesame-repo this) graph base-uri format triple-stream)))

(defn- add-statement-delegate
  ([this statement] (proto/add-statement (->sesame-repo this) statement))
  ([this graph statement] (proto/add-statement (->sesame-repo this) graph statement)))

(def itriple-writeable-delegate
  {:add add-delegate
   :add-statement add-statement-delegate})









(comment
  ;; experiments

  


  (let [at (atom [])
        listener (reify org.eclipse.rdf4j.repository.event.RepositoryConnectionListener
                   (add [this conn sub pred obj graphs]
                     (swap! at conj [:add sub pred obj graphs]))
                   (begin [this conn]
                     (swap! at conj [:begin]))
                   (close [this conn]
                     (swap! at conj [:close]))
                   (commit [this conn]
                     (swap! at conj [:commit])))
        
        repo (doto (grafter.rdf.repository/notifying-repo (grafter.rdf.repository/sparql-repo "http://localhost:5820/drafter-test-db/query" "http://localhost:5820/drafter-test-db/update"))
               (.addRepositoryConnectionListener listener))]

    (with-open [conn (grafter.rdf.repository/->connection repo)]
      (grafter.rdf/add conn [(grafter.rdf.protocols/->Quad (URI. "http://foo") (URI. "http://foo") (URI. "http://foo") (URI. "http://foo"))]))
    
    @at)

  ;; => [[:begin] [:add #object[org.eclipse.rdf4j.model.impl.URIImpl 0x4e213334 "http://foo"] #object[org.eclipse.rdf4j.model.impl.URIImpl 0x211a94a8 "http://foo"] #object[org.eclipse.rdf4j.model.impl.URIImpl 0x667e19c8 "http://foo"] #object["[Lorg.eclipse.rdf4j.model.Resource;" 0x2bfcfcbe "[Lorg.eclipse.rdf4j.model.Resource;@2bfcfcbe"]] [:commit] [:close]]
  
  
  )
