(ns drafter.rdf.draftset-management.jobs-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t]
            [drafter.backend.draftset.operations :as dsops]
            [drafter.rdf.drafter-ontology :as do :refer :all]
            [drafter.rdf.draftset-management.jobs :as sut]
            [drafter.rdf.sparql :as sparql]
            [drafter.test-common :as tc]
            [drafter.user-test :refer [test-editor]]
            [drafter.write-scheduler :as scheduler]
            [grafter.rdf :refer [->Triple]])
  (:import java.net.URI
           org.eclipse.rdf4j.rio.RDFFormat))

(defn apply-job!
  "Execute the job in this thread"
  [{fun :function :as job}]
  (let [ret (fun job)]
    (t/is (= true ret)
          "Successful job (returns true doesn't return an exception/error)")))

(defn fetch-draft-graph-modified-at [backend draftset live-graph]
  (let [ds-uri (do/draftset-id->uri (:id draftset))
        modified-query (str "SELECT ?modified {"
                            "   <" live-graph "> <" drafter:hasDraft "> ?draftgraph ."
                            "   ?draftgraph a <" drafter:DraftGraph "> ;"
                            "                 <" drafter:inDraftSet "> <" ds-uri "> ;"
                            "                 <" drafter:modifiedAt "> ?modified ."

                            "} LIMIT 2")]

    (let [res (-> backend
                  (sparql/eager-query modified-query))]

      (assert (= 1 (count res)) "There were multiple modifiedAt timestamps, we expect just one.")

      (-> res first :modified))))

(defn- get-graph-triples [backend graph-uri]
  (let [results (sparql/eager-query backend (tc/select-all-in-graph graph-uri))]
    (map (fn [{:keys [s p o]}] (->Triple s p o)) results)))


(tc/deftest-system append-data-to-draftset-job-test
  [{:keys [:drafter/backend]} "drafter/rdf/draftset-management/jobs.edn"]
  (let [ds (dsops/create-draftset! backend test-editor)]
    (apply-job! (sut/append-triples-to-draftset-job backend ds (io/file "./test/test-triple.nt") RDFFormat/NTRIPLES (URI. "http://foo/graph")))
    (let [ts-1 (fetch-draft-graph-modified-at backend ds "http://foo/graph")]
      (apply-job! (sut/append-triples-to-draftset-job backend ds (io/file "./test/test-triple-2.nt") RDFFormat/NTRIPLES (URI. "http://foo/graph")))
      (let [ts-2 (fetch-draft-graph-modified-at backend ds "http://foo/graph")]
        (t/is (< (.getTime ts-1)
                 (.getTime ts-2))
              "Modified time is updated after append")

        (apply-job! (sut/delete-triples-from-draftset-job backend ds (URI. "http://foo/graph") (io/file "./test/test-triple-2.nt") RDFFormat/NTRIPLES))
        (let [ts-3 (fetch-draft-graph-modified-at backend ds "http://foo/graph")]
          (t/is (< (.getTime ts-2)
                   (.getTime ts-3))
                "Modified time is updated after delete"))))))

(tc/deftest-system copy-live-graph-into-draftset-test
  ;; TODO port to use a test fixture
  [{:keys [:drafter/backend]} "drafter/rdf/draftset-management/jobs.edn"]
  (let [draftset-id (dsops/create-draftset! backend test-editor)
        live-triples (tc/test-triples (URI. "http://test-subject"))
        live-graph-uri (tc/make-graph-live! backend (URI. "http://live") live-triples (constantly #inst "2015"))
        {:keys [value-p] :as copy-job} (sut/copy-live-graph-into-draftset-job backend draftset-id live-graph-uri)]
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
        {:keys [value-p] :as copy-job} (sut/copy-live-graph-into-draftset-job backend draftset-id live-graph-uri)]

    (scheduler/queue-job! copy-job)
    @value-p

    (let [draft-triples (get-graph-triples backend draft-graph-uri)]
      (t/is (= (set live-triples) (set draft-triples))))))





