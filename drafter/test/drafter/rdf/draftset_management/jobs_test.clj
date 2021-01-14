(ns drafter.rdf.draftset-management.jobs-test
  (:require [clojure.test :as t]
            [drafter.backend.draftset.operations :as dsops]
            [drafter.feature.draftset-data.append-by-graph :as append-graph]
            [drafter.rdf.sparql :as sparql]
            [drafter.test-common :as tc]
            [drafter.user-test :refer [test-editor]]
            [grafter-2.rdf.protocols :refer [->Triple]]
            [drafter.backend.draftset.operations :as ops]
            [drafter.backend.draftset.draft-management :as mgmt])
  (:import java.net.URI))

(t/use-fixtures :each tc/with-spec-instrumentation)

(def dummy "dummy@user.com")

(defn- get-graph-triples [backend graph-uri]
  (let [results (sparql/eager-query backend (tc/select-all-in-graph graph-uri))]
    (map (fn [{:keys [s p o]}] (->Triple s p o)) results)))

(defn- job-resources [system]
  {:backend (:drafter/backend system)
   :global-writes-lock (:drafter/global-writes-lock system)
   :graph-manager (:drafter.backend.draftset.graphs/manager system)})

(t/deftest copy-live-graph-into-draftset-test
  (tc/with-system
    [{:keys [:drafter/backend :drafter/global-writes-lock] :as system} "drafter/rdf/draftset-management/jobs.edn"]
    (let [resources (job-resources system)
          draftset-id (dsops/create-draftset! backend test-editor)
          live-triples (tc/test-triples (URI. "http://test-subject"))
          live-graph-uri (tc/make-graph-live! backend (URI. "http://live") live-triples (constantly #inst "2015"))
          copy-job (append-graph/copy-live-graph-into-draftset-job
                     resources
                     dummy
                     {:draftset-id draftset-id
                      :graph       live-graph-uri})]
      (tc/exec-and-await-job-success copy-job)
      (let [draft-graph (dsops/find-draftset-draft-graph backend draftset-id live-graph-uri)
            draft-triples (get-graph-triples backend draft-graph)]
        (t/is (= (set live-triples) (set draft-triples)))))))

(t/deftest copy-live-graph-into-existing-draft-graph-in-draftset-test
  (tc/with-system
    [{:keys [:drafter/backend] :as system} "drafter/rdf/draftset-management/jobs.edn"]
    (let [resources (job-resources system)
          draftset-id (dsops/create-draftset! backend test-editor)
          live-triples (tc/test-triples (URI. "http://test-subject"))
          live-graph-uri (tc/make-graph-live! backend (URI. "http://live") live-triples (constantly #inst "2015"))
          initial-draft-triples (tc/test-triples (URI. "http://temp-subject"))
          draft-graph-uri (tc/import-data-to-draft! backend live-graph-uri initial-draft-triples draftset-id)
          copy-job (append-graph/copy-live-graph-into-draftset-job
                     resources
                     dummy
                     {:draftset-id draftset-id
                      :graph       live-graph-uri})]
      (tc/exec-and-await-job-success copy-job)
      (let [draft-triples (get-graph-triples backend draft-graph-uri)]
        (t/is (= (set live-triples) (set draft-triples)))))))

(t/deftest copy-live-graph-into-draftset-job-should-not-copy-protected-graphs
  (tc/with-system
    [{:keys [:drafter/backend :drafter/global-writes-lock] :as system} "drafter/rdf/draftset-management/jobs.edn"]
    (let [{:keys [backend] :as resources} (job-resources system)
          draftset-id (dsops/create-draftset! backend test-editor)
          protected-graph mgmt/drafter-state-graph
          params {:draftset-id draftset-id :graph protected-graph}
          job (append-graph/copy-live-graph-into-draftset-job resources dummy params)
          result (tc/exec-and-await-job job)
          draft-graph (ops/find-draftset-draft-graph backend draftset-id protected-graph)]
      (t/is (= :error (:type result)))
      (t/is (nil? draft-graph)))))
