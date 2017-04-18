(ns drafter.rdf.draft-management-test
  (:require [clojure.test :refer :all]
            [drafter
             [test-common :refer [*test-backend* ask? import-data-to-draft! make-graph-live! wrap-clean-test-db wrap-db-setup]]
             [user-test :refer [test-editor]]]
            [drafter.test-generators :as gen]
            [drafter.rdf
             [draft-management :refer :all]
             [drafter-ontology :refer :all]
             [draftset-management :refer [create-draftset!]]]
            [grafter.vocabularies.dcterms :refer [dcterms:issued dcterms:modified]]
            [drafter.test-helpers.draft-management-helpers :as mgmt]
            [drafter.draftset :refer [->DraftsetId]]
            [grafter.rdf :refer [add statements]]
            [grafter.rdf
             [repository :as repo]
             [templater :refer [triplify]]]
            [grafter.vocabularies.rdf :refer :all]
            [schema.test :refer [validate-schemas]]
            [selmer.parser :refer [render render-file]]
            [drafter.util :as util]
            [clojure.set :as set])
  (:import [java.util Date UUID]
           [java.net URI]))

(use-fixtures :each validate-schemas)

(defn- print-statement [s]
  (println (util/map-values str s)))

(def test-graph-uri (URI. "http://example.org/my-graph"))

(defn- assert-statements-exist [repo expected & {:keys [debug]}]
  (let [missing (set/difference (set expected) (set (statements repo)))]
    (when (and debug (not (empty? missing)))
      (println "Missing statements:")
      (doseq [q missing]
        (print-statement q))

      (println)

      (println "Repository statements:")
      (doseq [q (statements repo)]
        (print-statement q)))

    (is (empty? missing))))

(deftest is-graph-managed?-test
  (let [r (gen/generate-repository {:managed-graphs {test-graph-uri {:is-public false
                                                                     :drafts ::gen/gen}}})]
    (is (is-graph-managed? r test-graph-uri))))

(deftest create-managed-graph!-test
  (let [r (repo/repo)
        expected (gen/generate-statements {:managed-graphs {test-graph-uri {:is-public false :drafts {} :triples []}}
                                           :draftsets {}})]
    (create-managed-graph! r test-graph-uri)
    (assert-statements-exist r expected)))

(deftest is-graph-live?-test
  (testing "Non-existent graph"
    (let [r (repo/repo)]
      (is (= false (is-graph-live? r (URI. "http://missing"))))))

  (testing "Non-live graph"
    (let [repo (gen/generate-repository {:managed-graphs {test-graph-uri {:is-public false}}})]
      (is (= false (is-graph-live? repo test-graph-uri)))))

  (testing "Live graph"
    (let [repo (gen/generate-repository {:managed-graphs {test-graph-uri {:is-public true}}})]
      (is (is-graph-live? repo test-graph-uri)))))

(deftest create-draft-graph!-test
  (testing "within draft set"
    (let [draftset-id (UUID/randomUUID)
          ds-uri (draftset-id->uri draftset-id)
          r (gen/generate-repository {:managed-graphs {test-graph-uri {:is-public false
                                                                       :drafts {}}}})]
      (let [draft-graph-uri (create-draft-graph! r test-graph-uri (->DraftsetId draftset-id))]
        (mgmt/draft-exists? r draft-graph-uri)
        (is (= true (repo/query r (str
                                    "ASK WHERE {"
                                    "  GRAPH <" drafter-state-graph "> {"
                                    "    <" draft-graph-uri "> <" drafter:inDraftSet "> <" ds-uri "> ."
                                    "  }"
                                    "}"))))))))

(deftest lookup-live-graph-test
  (testing "lookup-live-graph"
    (let [draft-graph-uri (URI. "http://draft")
          r (gen/generate-repository {:managed-graphs {test-graph-uri {:is-public true :drafts {draft-graph-uri ::gen/gen}}}})
          found-graph-uri (lookup-live-graph r draft-graph-uri)]
      (is (= test-graph-uri found-graph-uri)))))

(deftest set-isPublic!-test
  (testing "set-isPublic!"
    (let [r (gen/generate-repository {:managed-graphs {test-graph-uri {:is-public false}}})
          expected (gen/generate-statements {:managed-graphs {test-graph-uri {:is-public true
                                                                              :triples []
                                                                              :drafts {}}}
                                             :draftsets {}})]
      (set-isPublic! r test-graph-uri true)
      (assert-statements-exist r expected))))

(deftest migrate-graphs-to-live!-test
  (testing "migrate-graphs-to-live! data is migrated"
    (let [draft-graph-uri (URI. "http://draft1")
          ss (gen/generate-statements {:managed-graphs {test-graph-uri {:is-public false
                                                                        :drafts {draft-graph-uri {:triples 5}}
                                                                        :triples []}}})
          expected-live-statements (map #(assoc % :c test-graph-uri) (filter #(= draft-graph-uri (:c %)) ss))
          db (doto (repo/repo) (add ss))]
      (migrate-graphs-to-live! db [draft-graph-uri])
      (is (not (repo/query db (str "ASK WHERE { GRAPH <" draft-graph-uri "> { ?s ?p ?o } }")))
          "Draft graph contents no longer exist.")

      (assert-statements-exist db expected-live-statements :debug true)

      (is (= false (mgmt/draft-exists? db draft-graph-uri))
          "Draft graph should be removed from the state graph")

      (is (= true (is-graph-managed? db test-graph-uri))
          "Live graph reference shouldn't have been deleted from state graph")

      (is (repo/query db (str "ASK WHERE { GRAPH <" drafter-state-graph "> {"
                              "  <http://example.org/my-graph> <" dcterms:modified "> ?modified ;"
                              "                                <" dcterms:issued "> ?published ."
                              "  }"
                              "}"))
          "Live graph should have a modified and issued time stamp"))))

(deftest migrate-graphs-to-live!-remove-live-as-well-test
  (testing "migrate-graphs-to-live! DELETION: Deleted draft removes live graph from state graph"
    (let [test-graph-to-delete-uri (URI. "http://example.org/my-other-graph1")
          graph-to-keep-uri (URI. "http://example.org/keep-me-a1")
          draft-graph-to-del-uri (URI. "http://draft1")
          draft-graph-to-keep-uri (URI. "http://draft2")
          r (gen/generate-repository {:managed-graphs {test-graph-to-delete-uri {:is-public true
                                                                                 :triples 6
                                                                                 :drafts {draft-graph-to-del-uri {:triples []}}}
                                                       graph-to-keep-uri {:is-public true
                                                                          :triples 5
                                                                          :drafts {draft-graph-to-keep-uri ::gen/gen}}}})]

      (migrate-graphs-to-live! r [draft-graph-to-del-uri])
      ;;live graph should be deleted
      (is (not (is-graph-managed? r test-graph-to-delete-uri)))

      ;;draft graph should be deleted
      (is (not (mgmt/draft-exists? r draft-graph-to-del-uri)))

      ;;unrelated draft graph should still exist
      (is (mgmt/draft-exists? r draft-graph-to-keep-uri))

      ;;unrelated live graph should still exist
      (is (is-graph-managed? r graph-to-keep-uri)))))

(deftest migrate-graphs-to-live!-dont-remove-state-when-other-drafts-test
  (testing "migrate-graphs-to-live! DELETION: Doesn't delete from state graph when there's multiple drafts"
    (let [deleting-live-uri (URI. "http://example.org/my-other-graph2")
          deleting-draft-1-uri (URI. "http://draft1")
          deleting-draft-2-uri (URI. "http://draft2")
          other-live-uri (URI. "http://example.org/keep-me-b2")
          other-live-draft-uri (URI. "http://draft3")
          r (gen/generate-repository {:managed-graphs {deleting-live-uri {:is-public true
                                                                                 :triples ::gen/gen
                                                                                 :drafts {deleting-draft-1-uri {:triples []}
                                                                                          deleting-draft-2-uri ::gen/gen}}
                                                       other-live-uri {:is-public true
                                                                           :triples ::gen/gen
                                                                           :drafts {other-live-draft-uri ::gen/gen}}}})]

      ;;migrate empty draft graph to live - this should delete the live graph data
      (migrate-graphs-to-live! r [deleting-draft-1-uri])

      ;;live graph should be empty
      (is (= true (graph-empty? r deleting-live-uri)))

      ;;live graph should still be managed
      (is (is-graph-managed? r deleting-live-uri))

      ;;migrated draft should be deleted
      (is (= false (mgmt/draft-exists? r deleting-draft-1-uri)))

      ;;other draft for live graph should still exist
      (is (= true (mgmt/draft-exists? r deleting-draft-2-uri)))

      ;;other live graph should still exist
      (is (= true (is-graph-managed? r other-live-uri)))

      ;;draft for other live graph should still exist
      (is (= true (mgmt/draft-exists? r other-live-draft-uri))))))

(deftest draft-graphs-test
  (let [live-1 (URI. "http://real/graph/1")
        live-2 (URI. "http://real/graph/2")
        draft-1 (URI. "http://draft1")
        draft-2 (URI. "http://draft2")
        r (gen/generate-repository {:managed-graphs {live-1 {:is-public false :drafts {draft-1 ::gen/gen}}
                                                     live-2 {:is-public false :drafts {draft-2 ::gen/gen}}}})]

       (testing "draft-graphs returns all draft graphs"
         (is (= #{draft-1 draft-2} (mgmt/draft-graphs r))))

       (testing "live-graphs returns all live graphs"
         (is (= #{live-1 live-2} (live-graphs r :online false))))))

(deftest build-draft-map-test
  (let [live-1 (URI. "http://live1")
        live-2 (URI. "http://live2")
        draft-1 (URI. "http://draft1")
        draft-2 (URI. "http://draft2")
        db (gen/generate-repository {:managed-graphs {live-1 {:drafts {draft-1 ::gen/gen}}
                                                      live-2 {:drafts {draft-2 ::gen/gen}}}
                                     :draftsets {}})
        gm (graph-map db #{draft-1 draft-2})]
    (is (= {live-1 draft-1 live-2 draft-2} gm))))

(deftest upsert-single-object-insert-test
  (let [db (repo/repo)]
    (upsert-single-object! db "http://foo/" "http://bar/" "baz")
    (is (repo/query db "ASK { GRAPH <http://publishmydata.com/graphs/drafter/drafts> { <http://foo/> <http://bar/> \"baz\"} }"))))

(deftest upsert-single-object-update-test
  (let [db (repo/repo)
        subject (URI. "http://example.com/subject")
        predicate (URI. "http://example.com/predicate")]
    (add db (triplify [subject [predicate "initial"]]))
    (upsert-single-object! db subject predicate "updated")
    (is (repo/query db (str "ASK { GRAPH <http://publishmydata.com/graphs/drafter/drafts> {"
                       "<" subject "> <" predicate "> \"updated\""
                       "} }")))))

(deftest ensure-draft-graph-exists-for-test
  (testing "Draft graph already exists for live graph"
    (let [draft-graph-uri (URI. "http://draft")
          live-graph-uri (URI. "http://live")
          ds-uri (URI. "http://draftset")
          initial-statements (gen/generate-statements {:managed-graphs {live-graph-uri {:is-public false
                                                                                        :drafts {draft-graph-uri {:draftset-uri ds-uri}}}}})
          r (doto (repo/repo) (add initial-statements))
          initial-mapping {live-graph-uri draft-graph-uri}
          {found-draft-uri :draft-graph-uri graph-map :graph-map} (ensure-draft-exists-for r live-graph-uri initial-mapping ds-uri)]
      (is (= (set initial-statements) (set (statements r))))
      (is (= draft-graph-uri found-draft-uri))
      (is (= initial-mapping graph-map))))

  (testing "Live graph exists without draft"
    (let [r (gen/generate-repository {:managed-graphs {test-graph-uri {:is-public false
                                                                       :drafts {}}}})
          ds-uri (URI. "http://draftset")
          {:keys [draft-graph-uri graph-map]} (ensure-draft-exists-for r test-graph-uri {} ds-uri)]
      (mgmt/draft-exists? r draft-graph-uri)
      (is (= {test-graph-uri draft-graph-uri} graph-map))))

  (testing "Live graph does not exist"
    (let [ds-uri (URI. "http://draftset")
          r (repo/repo)
          {:keys [draft-graph-uri graph-map]} (ensure-draft-exists-for r test-graph-uri {} ds-uri)]
      (is-graph-managed? r test-graph-uri)
      (mgmt/draft-exists? r draft-graph-uri)
      (is (= {test-graph-uri draft-graph-uri} graph-map)))))

(deftest delete-draft-graph!-test
  (testing "only draft for non-live graph"
    (let [draft-graph-uri (URI. "http://draft-graph")
          r (gen/generate-repository {:managed-graphs {test-graph-uri {:is-public false
                                                                       :drafts {draft-graph-uri ::gen/gen}}}})]
      (delete-draft-graph! r draft-graph-uri)

      ;;draft and live graph should be deleted
      (is (= false (is-graph-managed? r test-graph-uri)))
      (is (= false (mgmt/draft-exists? r draft-graph-uri)))))

  (testing "only draft for live graph"
    (let [draft-graph-uri (URI. "http://draft-graph")
          r (gen/generate-repository {:managed-graphs {test-graph-uri {:is-public true
                                                                       :drafts {draft-graph-uri ::gen/gen}}}})]
      (delete-draft-graph! r draft-graph-uri)
      (is (= true (is-graph-managed? r test-graph-uri)))
      (is (= false (mgmt/draft-exists? r draft-graph-uri)))))

  (testing "Draft for managed graph with other graphs"
    (let [live-graph-uri (create-managed-graph! *test-backend* (URI. "http://live"))
          draft-graph-1 (URI. "http://draft1")
          draft-graph-2 (URI. "http://draft2")
          r (gen/generate-repository {:managed-graphs {live-graph-uri {:drafts {draft-graph-1 ::gen/gen
                                                                                draft-graph-2 ::gen/gen}}}})]

      (delete-draft-graph! r draft-graph-2)

      (is (= true (mgmt/draft-exists? r draft-graph-1)))
      (is (= true (is-graph-managed? r live-graph-uri))))))

;; This test attempts to capture the rationale behind the calculation of graph
;; restrictions.
;;
;; These tests attempt to recreate the various permutations of what will happen
;; when union-with-live=true/false and when there are drafts specified or not.
(deftest calculate-graph-restriction-test
  (testing "union-with-live=true"
    (testing "with no drafts specified"
      (is (= #{:l1 :l2}
             (calculate-graph-restriction #{:l1 :l2} #{} #{}))
          "Restriction should be the set of public live graphs"))

    (testing "with drafts specified"
      (is (= #{:l1 :d2 :d3 :d4}
             (calculate-graph-restriction #{:l1 :l2} #{:l3 :l4 :l2} #{:d2 :d3 :d4}))
          "Restriction should be the set of live graphs plus drafts from the
            draftset.  Live graphs for the specified drafts should not be
            included as we want to use their draft graphs.")))

  (testing "union-with-live=false"
    (testing "with no drafts specified"
      (is (= #{}
             (calculate-graph-restriction #{} #{} #{}))
          "Restriction should be the set of public live graphs"))
    (testing "with drafts specified"
      (is (= #{:d1 :d2}
             (calculate-graph-restriction #{} #{:l1 :l2} #{:d1 :d2}))
          "Restriction should be the set of drafts (as live graph queries will be rewritten to their draft graphs)"))))

(deftest set-timestamp-test
  (let [draft-graph-uri (URI. "http://draft-graph")
        modified-date (Date.)
        r (gen/generate-repository {:managed-graphs {test-graph-uri {:drafts {draft-graph-uri ::gen/gen}}}})]
    (set-modifed-at-on-draft-graph! r draft-graph-uri modified-date)

    (is (repo/query r
               (str
                "ASK {"
                "<" draft-graph-uri "> <" drafter:modifiedAt "> ?modified . "
                "}")))))

(deftest copy-graph-test
  (let [source-graph-uri (URI. "http://test-graph/1")
        dest-graph-uri (URI. "http://test-graph/2")
        initial-statements (gen/generate-statements {:managed-graphs {source-graph-uri {:triples 10}}})
        expected-dest-graph-statements (map #(assoc % :c dest-graph-uri) (filter #(= source-graph-uri (:c %)) initial-statements))
        repo (doto (repo/repo) (add initial-statements))]
    (copy-graph repo source-graph-uri dest-graph-uri)
    (assert-statements-exist repo expected-dest-graph-statements :debug true)))

(use-fixtures :once wrap-db-setup)
(use-fixtures :each wrap-clean-test-db)
