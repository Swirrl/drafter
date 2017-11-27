(ns drafter.rdf.draftset-management.jobs-test
  (:require [clojure.test :as t]
            [drafter.rdf.draftset-management.jobs :as sut])
  (:import java.net.URI
           org.eclipse.rdf4j.rio.RDFFormat))

(use-fixtures :each (wrap-system-setup "test-system.edn" [:drafter.backend/rdf4j-repo :drafter/write-scheduler]))
;(use-fixtures :each wrap-clean-test-db)

(defn apply-job! [{fun :function :as job}]
  (fun job))

(defn fetch-draft-graph-modified-at [draftset live-graph]
  (let [ds-uri (draftset-id->uri (:id draftset))
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
  (let [results (sparql/eager-query *test-backend* (select-all-in-graph graph-uri))]
    (map (fn [{:keys [s p o]}] (->Triple s p o)) results)))



(deftest append-data-to-draftset-job-test
  (let [ds (dsops/create-draftset! *test-backend* test-editor)]
    (apply-job! (sut/append-triples-to-draftset-job *test-backend* ds (io/file "./test/test-triple.nt") RDFFormat/NTRIPLES (URI. "http://foo/graph")))
    (let [ts-1 (fetch-draft-graph-modified-at ds "http://foo/graph")]
      (apply-job! (sut/append-triples-to-draftset-job *test-backend* ds (io/file "./test/test-triple-2.nt") RDFFormat/NTRIPLES (URI. "http://foo/graph")))
      (let [ts-2 (fetch-draft-graph-modified-at ds "http://foo/graph")]
        (is (< (.getTime ts-1)
               (.getTime ts-2))
            "Modified time is updated after append")

        (apply-job! (sut/delete-triples-from-draftset-job *test-backend* ds (URI. "http://foo/graph") (io/file "./test/test-triple-2.nt") RDFFormat/NTRIPLES))
        (let [ts-3 (fetch-draft-graph-modified-at ds "http://foo/graph")]
          (is (< (.getTime ts-2)
               (.getTime ts-3))
              "Modified time is updated after delete"))))))

(deftest copy-live-graph-into-draftset-test
  (let [draftset-id (dsops/create-draftset! *test-backend* test-editor)
        live-triples (test-triples (URI. "http://test-subject"))
        live-graph-uri (make-graph-live! *test-backend* (URI. "http://live") live-triples)
        {:keys [value-p] :as copy-job} (sut/copy-live-graph-into-draftset-job *test-backend* draftset-id live-graph-uri)]
    (scheduler/queue-job! copy-job)

    @value-p

    (let [draft-graph (dsops/find-draftset-draft-graph *test-backend* draftset-id live-graph-uri)
          draft-triples (get-graph-triples draft-graph)]
      (is (= (set live-triples) (set draft-triples))))))

(deftest copy-live-graph-into-existing-draft-graph-in-draftset-test
  (let [draftset-id (dsops/create-draftset! *test-backend* test-editor)
        live-triples (test-triples (URI. "http://test-subject"))
        live-graph-uri (make-graph-live! *test-backend* (URI. "http://live") live-triples)
        initial-draft-triples (test-triples (URI. "http://temp-subject"))
        draft-graph-uri (import-data-to-draft! *test-backend* live-graph-uri initial-draft-triples draftset-id)
        {:keys [value-p] :as copy-job} (sut/copy-live-graph-into-draftset-job *test-backend* draftset-id live-graph-uri)]

    (scheduler/queue-job! copy-job)
    @value-p

    (let [draft-triples (get-graph-triples draft-graph-uri)]
      (is (= (set live-triples) (set draft-triples))))))



