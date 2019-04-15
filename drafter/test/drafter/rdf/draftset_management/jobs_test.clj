(ns drafter.rdf.draftset-management.jobs-test
  (:require [clojure.test :as t]
            [drafter.backend.draftset.operations :as dsops]
            [drafter.feature.draftset-data.append-by-graph :as append-graph]
            [drafter.rdf.sparql :as sparql]
            [drafter.test-common :as tc]
            [drafter.user-test :refer [test-editor]]
            [drafter.write-scheduler :as scheduler]
            [grafter-2.rdf.protocols :refer [->Triple]])
  (:import java.net.URI))

(t/use-fixtures :each tc/with-spec-instrumentation)

(defn- get-graph-triples [backend graph-uri]
  (let [results (sparql/eager-query backend (tc/select-all-in-graph graph-uri))]
    (map (fn [{:keys [s p o]}] (->Triple s p o)) results)))


(tc/deftest-system copy-live-graph-into-draftset-test
  ;; TODO port to use a test fixture
  [{:keys [:drafter/backend]} "drafter/rdf/draftset-management/jobs.edn"]
  (let [draftset-id (dsops/create-draftset! backend test-editor)
        live-triples (tc/test-triples (URI. "http://test-subject"))
        live-graph-uri (tc/make-graph-live! backend (URI. "http://live") live-triples (constantly #inst "2015"))
        {:keys [value-p] :as copy-job} (append-graph/copy-live-graph-into-draftset-job backend draftset-id live-graph-uri)]
    (scheduler/queue-job! copy-job)

    @value-p

    (let [draft-graph (dsops/find-draftset-draft-graph backend draftset-id live-graph-uri)
          draft-triples (get-graph-triples backend draft-graph)]
      (t/is (= (set live-triples) (set draft-triples))))))

(tc/deftest-system copy-live-graph-into-existing-draft-graph-in-draftset-test
  [{:keys [:drafter/backend]} "drafter/rdf/draftset-management/jobs.edn"]
  (let [draftset-id (dsops/create-draftset! backend test-editor)
        live-triples (tc/test-triples (URI. "http://test-subject"))
        live-graph-uri (tc/make-graph-live! backend (URI. "http://live") live-triples (constantly #inst "2015"))
        initial-draft-triples (tc/test-triples (URI. "http://temp-subject"))
        draft-graph-uri (tc/import-data-to-draft! backend live-graph-uri initial-draft-triples draftset-id (constantly #inst "2016"))
        {:keys [value-p] :as copy-job} (append-graph/copy-live-graph-into-draftset-job backend draftset-id live-graph-uri)]

    (scheduler/queue-job! copy-job)
    @value-p

    (let [draft-triples (get-graph-triples backend draft-graph-uri)]
      (t/is (= (set live-triples) (set draft-triples))))))
