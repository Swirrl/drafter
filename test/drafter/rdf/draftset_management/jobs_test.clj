(ns drafter.rdf.draftset-management.jobs-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t]
            [drafter.backend.draftset.operations :as dsops]
            [drafter.rdf.drafter-ontology :as do :refer :all]
            [drafter.rdf.draftset-management.jobs :as sut]
            [drafter.rdf.sparql :as sparql]
            [drafter.test-common :as tc :refer [*test-backend*]]
            [drafter.user-test :refer [test-editor]]
            [drafter.write-scheduler :as scheduler]
            [grafter.rdf :refer [->Triple]])
  (:import java.net.URI
           org.eclipse.rdf4j.rio.RDFFormat))

(t/use-fixtures :each (tc/wrap-system-setup "test-system.edn" [:drafter.backend/rdf4j-repo :drafter/write-scheduler]))
;(use-fixtures :each wrap-clean-test-db)

(defn apply-job! [{fun :function :as job}]
  (fun job))

(defn fetch-draft-graph-modified-at [draftset live-graph]
  (let [ds-uri (do/draftset-id->uri (:id draftset))
        modified-query (str "SELECT ?modified {"
                            "   <" live-graph "> <" drafter:hasDraft "> ?draftgraph ."
                            "   ?draftgraph a <" drafter:DraftGraph "> ;"
                            "                 <" drafter:inDraftSet "> <" ds-uri "> ;"
                            "                 <" drafter:modifiedAt "> ?modified ."

                            "} LIMIT 2")]

    (let [res (-> *test-backend*
                  (sparql/eager-query modified-query))]

      (assert (= 1 (count res)) "There were multiple modifiedAt timestamps, we expect just one.")

      (-> res first :modified))))

(defn- get-graph-triples [graph-uri]
  (let [results (sparql/eager-query *test-backend* (tc/select-all-in-graph graph-uri))]
    (map (fn [{:keys [s p o]}] (->Triple s p o)) results)))



(t/deftest append-data-to-draftset-job-test
  (let [ds (dsops/create-draftset! *test-backend* test-editor)]
    (apply-job! (sut/append-triples-to-draftset-job *test-backend* ds (io/file "./test/test-triple.nt") RDFFormat/NTRIPLES (URI. "http://foo/graph")))
    (let [ts-1 (fetch-draft-graph-modified-at ds "http://foo/graph")]
      (apply-job! (sut/append-triples-to-draftset-job *test-backend* ds (io/file "./test/test-triple-2.nt") RDFFormat/NTRIPLES (URI. "http://foo/graph")))
      (let [ts-2 (fetch-draft-graph-modified-at ds "http://foo/graph")]
        (t/is (< (.getTime ts-1)
               (.getTime ts-2))
            "Modified time is updated after append")

        (apply-job! (sut/delete-triples-from-draftset-job *test-backend* ds (URI. "http://foo/graph") (io/file "./test/test-triple-2.nt") RDFFormat/NTRIPLES))
        (let [ts-3 (fetch-draft-graph-modified-at ds "http://foo/graph")]
          (t/is (< (.getTime ts-2)
               (.getTime ts-3))
              "Modified time is updated after delete"))))))

(t/deftest copy-live-graph-into-draftset-test
  (let [draftset-id (dsops/create-draftset! *test-backend* test-editor)
        live-triples (tc/test-triples (URI. "http://test-subject"))
        live-graph-uri (tc/make-graph-live! *test-backend* (URI. "http://live") live-triples)
        {:keys [value-p] :as copy-job} (sut/copy-live-graph-into-draftset-job *test-backend* draftset-id live-graph-uri)]
    (scheduler/queue-job! copy-job)

    @value-p

    (let [draft-graph (dsops/find-draftset-draft-graph *test-backend* draftset-id live-graph-uri)
          draft-triples (get-graph-triples draft-graph)]
      (t/is (= (set live-triples) (set draft-triples))))))

(t/deftest copy-live-graph-into-existing-draft-graph-in-draftset-test
  (let [draftset-id (dsops/create-draftset! *test-backend* test-editor)
        live-triples (tc/test-triples (URI. "http://test-subject"))
        live-graph-uri (tc/make-graph-live! *test-backend* (URI. "http://live") live-triples)
        initial-draft-triples (tc/test-triples (URI. "http://temp-subject"))
        draft-graph-uri (tc/import-data-to-draft! *test-backend* live-graph-uri initial-draft-triples draftset-id)
        {:keys [value-p] :as copy-job} (sut/copy-live-graph-into-draftset-job *test-backend* draftset-id live-graph-uri)]

    (scheduler/queue-job! copy-job)
    @value-p

    (let [draft-triples (get-graph-triples draft-graph-uri)]
      (t/is (= (set live-triples) (set draft-triples))))))



