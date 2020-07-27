(ns ^:rest-api drafter.feature.draftset.update-test
  (:require [clojure.test :as t :refer [is]]
            [grafter-2.rdf4j.io :refer [rdf-writer statements]]
            [grafter-2.rdf.protocols :refer [add context ->Quad ->Triple map->Triple]]
            [drafter.test-common :as tc]
            [drafter.user-test :refer [test-editor test-manager test-password test-publisher]]
            [drafter.feature.draftset.test-helper :as help]
            [drafter.util :as util])
  (:import org.eclipse.rdf4j.query.QueryResultHandler
           org.eclipse.rdf4j.query.resultio.sparqljson.SPARQLResultsJSONParser))

(t/use-fixtures :each tc/with-spec-instrumentation)

(def system-config "drafter/feature/empty-db-system.edn")

;; (defn- result-set-handler [result-state]
;;   (reify QueryResultHandler
;;     (handleBoolean [this b])
;;     (handleLinks [this links])
;;     (startQueryResult [this binding-names])
;;     (endQueryResult [this])
;;     (handleSolution [this binding-set]
;;       (let [binding-pairs (map (fn [b] [(keyword (.getName b)) (.stringValue (.getValue b))]) binding-set)
;;             binding-map (into {} binding-pairs)]
;;         (swap! result-state conj binding-map)))))

(defn- create-update-request
  [user draftset-location accept-content-type stmt]
  (tc/with-identity user
    {:uri (str draftset-location "/update")
     :headers {"accept" accept-content-type}
     :request-method :post
     :body stmt}))

;; (defn- select-query-draftset-through-api [handler user draftset-location select-query & {:keys [union-with-live?]}]
;;   (let [request (create-query-request user draftset-location select-query "application/sparql-results+json" :union-with-live? union-with-live?)
;;         {:keys [body] :as query-response} (handler request)]
;;     (tc/assert-is-ok-response query-response)
;;     (let [result-state (atom #{})
;;           result-handler (result-set-handler result-state)
;;           parser (doto (SPARQLResultsJSONParser.) (.setQueryResultHandler result-handler))]

;;       (.parse parser body)
;;       @result-state)))

(tc/with-system
  [:drafter.fixture-data/loader [:drafter/routes :draftset/api] :drafter/write-scheduler]
  [system system-config]
  (let [handler (get system [:drafter/routes :draftset/api])
        draftset-location (help/create-draftset-through-api handler test-editor)]
    (let [stmt "INSERT DATA { GRAPH <http://g> { <http://s> <http://p> <http://o> } }"
          request (create-update-request test-editor
                                         draftset-location
                                         "text/plain"
                                         stmt)
          response (handler request)]
      (clojure.pprint/pprint response))))
