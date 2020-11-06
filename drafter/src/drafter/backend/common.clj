(ns drafter.backend.common
  (:require [drafter.backend.draftset.arq :as arq]
            [grafter-2.rdf4j.repository :as repo])
  (:import java.net.URI))

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

(defn validate-query
  "Validates a query by parsing it using ARQ. If the query is invalid
  a QueryParseException is thrown."
  [query-str]
  (arq/sparql-string->arq-query query-str)
  query-str)

(defn prep-and-validate-query [conn sparql-string]
  (let [;; Technically calls to live endpoint don't need to be
        ;; validated with JENA/ARQ but as draftsets do their rewriting
        ;; through ARQ this helps ensure consistency between
        ;; implementations.
        validated-query-string (validate-query sparql-string)]
    (repo/prepare-query conn validated-query-string)))

(defn user-dataset [{:keys [default-graph-uri named-graph-uri] :as sparql}]
  (when (or (seq default-graph-uri) (seq named-graph-uri))
    (repo/make-restricted-dataset :default-graph default-graph-uri
                                  :named-graphs named-graph-uri)))

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
      (grafter.rdf/add conn [(grafter.core/->Quad (URI. "http://foo") (URI. "http://foo") (URI. "http://foo") (URI. "http://foo"))]))

    @at)

  ;; => [[:begin] [:add #object[org.eclipse.rdf4j.model.impl.URIImpl 0x4e213334 "http://foo"] #object[org.eclipse.rdf4j.model.impl.URIImpl 0x211a94a8 "http://foo"] #object[org.eclipse.rdf4j.model.impl.URIImpl 0x667e19c8 "http://foo"] #object["[Lorg.eclipse.rdf4j.model.Resource;" 0x2bfcfcbe "[Lorg.eclipse.rdf4j.model.Resource;@2bfcfcbe"]] [:commit] [:close]]


  )
