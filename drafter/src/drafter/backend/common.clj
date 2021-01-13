(ns drafter.backend.common
  (:require [drafter.backend.draftset.arq :as arq]
            [grafter-2.rdf4j.repository :as repo]
            [clojure.set :as set])
  (:import java.net.URI
           org.eclipse.rdf4j.query.Dataset
           org.apache.jena.sparql.core.DatasetDescription))

(defn- get-restrictions [graph-restrictions]
  (cond
   (coll? graph-restrictions) graph-restrictions
   (fn? graph-restrictions) (graph-restrictions)
   :else nil))

(defn restricted-dataset
  "Returns a restricted dataset or nil when given either a 0-arg
  function or a collection of graph uris."
  [graph-restrictions]
  {:pre [(or (nil? graph-restrictions)
             (coll? graph-restrictions)
             (fn? graph-restrictions))]
   :post [(or (instance? Dataset %)
              (nil? %))]}
  (when-let [graph-restrictions (get-restrictions graph-restrictions)]
    (let [stringified-restriction (map str graph-restrictions)]
      (repo/make-restricted-dataset :default-graph stringified-restriction
                                    :named-graphs stringified-restriction))))

(defn apply-restriction [pquery restriction]
  (let [dataset (restricted-dataset restriction)]
    (.setDataset pquery dataset)
    pquery))

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
