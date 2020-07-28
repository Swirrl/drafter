(ns ^:rest-api drafter.feature.draftset.update-test
  (:require [clojure.test :as t :refer [is]]
            [grafter-2.rdf4j.io :refer [rdf-writer statements]]
            [grafter-2.rdf.protocols :refer [add context ->Quad ->Triple map->Triple]]
            [drafter.test-common :as tc]
            [drafter.user-test :refer [test-editor test-manager test-password test-publisher]]
            [drafter.feature.draftset.test-helper :as help]
            [drafter.util :as util]
            [grafter-2.rdf4j.repository :as repo]
            [drafter.draftset :as ds]
            [clojure.string :as string]
            [grafter-2.rdf.protocols :as pr])
  (:import java.net.URI
           org.eclipse.rdf4j.query.QueryResultHandler
           org.eclipse.rdf4j.query.resultio.sparqljson.SPARQLResultsJSONParser))

(t/use-fixtures :each tc/with-spec-instrumentation)

(def system-config "drafter/feature/empty-db-system.edn")

(def keys-for-test
  [[:drafter/routes :draftset/api]
   :drafter/backend
   :drafter/write-scheduler
   :drafter.routes.sparql/live-sparql-query-route
   :drafter.backend.live/endpoint
   :drafter.common.config/sparql-query-endpoint])

(defn- create-update-request
  [user draftset-location accept-content-type stmt]
  (tc/with-identity user
    {:uri (str draftset-location "/update")
     :headers {"accept" accept-content-type}
     :request-method :post
     :body stmt}))

(tc/deftest-system-with-keys insert-modify-test
  keys-for-test [system system-config]
  (let [handler (get system [:drafter/routes :draftset/api])
        draftset-location (help/create-draftset-through-api handler test-editor)
        draftset-id (last (string/split draftset-location #"/"))
        draftset-uri (ds/->draftset-uri draftset-id)
        update! (fn [stmt]
                  (handler (create-update-request
                            test-editor draftset-location "text/plain" stmt)))]
    (let [stmt "
INSERT DATA { GRAPH <http://g> { <http://s> <http://p> <http://o> } }
"
          response (update! stmt)
          _ (tc/assert-is-no-content-response response)
          stmt "
DELETE DATA { GRAPH <http://g> { <http://s> <http://p> <http://o> } } ;
INSERT DATA { GRAPH <http://g> { <http://s> <http://p> <http://g> } }
"
          response (update! stmt)
          _ (tc/assert-is-no-content-response response)

          q "
SELECT ?dg ?s ?p ?o WHERE {
  GRAPH ?dg { ?s ?p ?o }
  GRAPH <http://publishmydata.com/graphs/drafter/drafts> {
    BIND ( <%s> AS ?ds )
    ?ds <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>
      <http://publishmydata.com/def/drafter/DraftSet> .
    ?dg <http://publishmydata.com/def/drafter/inDraftSet> ?ds .
    ?lg <http://publishmydata.com/def/drafter/hasDraft> ?dg .
  }
}"]
      (let [[{:keys [dg s p o]} :as quads]
            (with-open [conn (-> system
                                 :drafter.common.config/sparql-query-endpoint
                                 repo/sparql-repo
                                 repo/->connection)]
              (repo/query conn (format q draftset-uri)))]
        (is (= 1 (count quads)))
        (is (= dg o))
        (is (not= o (URI. "http://g")))))))

(def valid-triples
  (->> (range)
       (map (fn [i]
              [(str "http://g/" i)
               (str "http://s/" i)
               (str "http://p/" i)
               (str "http://g/" i)]))))

(defn quad-pattern-str [quad]
  (apply format "GRAPH <%s> { <%s> <%s> <%s> }" quad))

(defn insert-stmt-str [quads]
  (str "INSERT DATA { "
       (string/join " . \n" (map quad-pattern-str quads))
       "}"))
