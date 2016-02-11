(ns drafter.rdf.draftset-management-test
  (:require [clojure.test :refer :all]
            [drafter.rdf.draftset-management :refer :all]
            [drafter.rdf.draft-management :refer [draft-exists?] :as mgmt]
            [drafter.test-common :refer [*test-backend* wrap-db-setup wrap-clean-test-db ask? import-data-to-draft! make-graph-live! test-triples
                                         test-editor test-publisher]]
            [grafter.rdf :refer [statements context]]
            [drafter.rdf.drafter-ontology :refer :all :as ont]
            [drafter.user :as user]
            [drafter.util :as util]
            [grafter.rdf.repository :refer [query]]
            [grafter.vocabularies.rdf :refer :all]))

(defn- has-uri-object? [s p uri-o]
  (query *test-backend* (str "ASK WHERE { <" s "> <" p "> <" uri-o "> }")))

(defn- has-string-object? [s p str-o]
  (query *test-backend* (str "ASK WHERE { <" s "> <" p "> \"" str-o "\" }")))

(defn- has-any-object? [s p]
  (query *test-backend* (str "ASK WHERE { <" s "> <" p "> ?o }")))

(defn- assert-user-is-creator-and-owner [{:keys [email] :as user} draftset-id]
  (is (has-string-object? (->draftset-uri draftset-id) drafter:createdBy email))
  (is (has-string-object? (->draftset-uri draftset-id) drafter:hasOwner email)))

(deftest create-draftset!-test
  (let [title "Test draftset"
        description "Test description"]
    (testing "Without title or description"
      (let [draftset-id (create-draftset! *test-backend* test-editor)
            ds-uri (->draftset-uri draftset-id)]
        (is (draftset-exists? *test-backend* ds-uri))
        (is (= false (has-any-object? ds-uri rdfs:label)))
        (is (= false (has-any-object? ds-uri rdfs:comment)))
        (assert-user-is-creator-and-owner test-editor draftset-id)))

    (testing "Without description"
      (let [draftset-id (create-draftset! *test-backend* test-editor title)
            ds-uri (->draftset-uri draftset-id)]
        (is (has-uri-object? ds-uri rdf:a drafter:DraftSet))
        (is (has-string-object? ds-uri rdfs:label title))
        (is (ask? "<" ds-uri "> <" drafter:createdAt "> ?o"))
        (assert-user-is-creator-and-owner test-editor draftset-id)))

    (testing "With description"
      (let [draftset-id (create-draftset! *test-backend* test-editor title description)
            ds-uri (->draftset-uri draftset-id)]
        (is (has-uri-object? ds-uri rdf:a drafter:DraftSet))
        (is (has-string-object? ds-uri rdfs:label title))
        (is (has-string-object? ds-uri rdfs:comment description))
        (is (ask? "<" ds-uri "> <" drafter:createdAt "> ?o"))
        (assert-user-is-creator-and-owner test-editor draftset-id)))))

(deftest draftset-exists-test
  (testing "Existing draftset"
    (let [draftset-id (create-draftset! *test-backend* test-editor "Test draftset")]
      (is (draftset-exists? *test-backend* draftset-id))))

  (testing "Non-existent draftset"
    (is (= false (draftset-exists? *test-backend* (->DraftsetId "missing"))))))

(deftest get-draftest-owner
  (testing "With owner"
    (let [draftset-id (create-draftset! *test-backend* test-editor)
          owner (get-draftset-owner *test-backend* draftset-id)]
      (is (= (user/username test-editor) owner))))

  (testing "With no owner"
    (let [draftset-id (create-draftset! *test-backend* test-editor)]
      (offer-draftset! *test-backend* draftset-id test-editor :publisher)
      (let [owner (get-draftset-owner *test-backend* draftset-id)]
        (is (nil? owner))))))

(deftest is-draftset-owner?-test
  (testing "Is owner"
    (let [draftset-id (create-draftset! *test-backend* test-editor)]
      (is (= true (is-draftset-owner? *test-backend* test-editor draftset-id)))))
  
  (testing "Has no owner"
    (let [draftset-id (create-draftset! *test-backend* test-editor)]
      (offer-draftset! *test-backend* draftset-id test-editor :publisher)
      (is (= false (is-draftset-owner? *test-backend* test-editor draftset-id)))))
  
  (testing "Has different owner"
    (let [draftset-id (create-draftset! *test-backend* test-editor)]
      (is (= false (is-draftset-owner? *test-backend* test-publisher draftset-id))))))

(deftest delete-draftset-statements!-test
  (let [draftset-id (create-draftset! *test-backend* test-editor "Test draftset")]
    (delete-draftset-statements! *test-backend* draftset-id)
    (is (= false (ask? (str "<" draftset-uri ">") "?p" "?o")))))

(defn- import-data-to-draftset! [db draftset-id quads]
  (let [graph-quads (group-by context quads)]
    (doall (map (fn [[live qs]] (import-data-to-draft! *test-backend* live qs draftset-id)) graph-quads))))

(deftest delete-draftset!-test
  (let [draftset-id (create-draftset! *test-backend* test-editor "Test draftset")
        quads (statements "test/resources/test-draftset.trig")
        draft-graphs (import-data-to-draftset! *test-backend* draftset-id quads)]
    
    (doseq [dg draft-graphs]
      (is (= true (draft-exists? *test-backend* dg))))

    (delete-draftset! *test-backend* draftset-id)

    (is (= false (draftset-exists? *test-backend* draftset-id)))

    (doseq [dg draft-graphs]
      (is (= false (draft-exists? *test-backend* dg))))))

(deftest delete-draftest-graph!-test
  (testing "Delete non-existent live graph"
    (let [draftset-id (create-draftset! *test-backend* test-editor "Test draftset")
          graph-to-delete "http://missing"]
      (delete-draftset-graph! *test-backend* draftset-id graph-to-delete)

      (is (= false (mgmt/is-graph-managed? *test-backend* graph-to-delete)))
      (is (empty? (get-draftset-graph-mapping *test-backend* draftset-id)))))

  (testing "Delete live graph not already in draftset"
    (let [live-graph "http://live"
          draftset-id (create-draftset! *test-backend* test-editor "Test draftset")]
      (make-graph-live! *test-backend* live-graph)
      (delete-draftset-graph! *test-backend* draftset-id live-graph)

      (is (mgmt/is-graph-managed? *test-backend* live-graph))

      (let [graph-mapping (get-draftset-graph-mapping *test-backend* draftset-id)]
        (is (contains? graph-mapping live-graph))

        (is (mgmt/draft-exists? *test-backend* (get graph-mapping live-graph))))))

  (testing "Graph already in draftset"
    (let [live-graph "http://live"
          draftset-id (create-draftset! *test-backend* test-editor "Test draftset")]
      (make-graph-live! *test-backend* live-graph)

      (let [draft-graph (import-data-to-draft! *test-backend* live-graph (test-triples "http://subject") draftset-id)]
        (is (mgmt/draft-exists? *test-backend* draft-graph))

        (delete-draftset-graph! *test-backend* draftset-id live-graph)

        (is (mgmt/draft-exists? *test-backend* draft-graph))
        (is (= false (ask? (format "GRAPH <%s> { ?s ?p ?o }" draft-graph))))))))

(deftest offer-draftset-test!
  (testing "Existing owner"
    (let [draftset-id (create-draftset! *test-backend* test-editor "Test draftset")
          draftset-uri (->draftset-uri draftset-id)]
      (offer-draftset! *test-backend* draftset-id test-editor :publisher)

      (has-string-object? draftset-uri drafter:claimableBy "publisher")
      (is (= false (has-any-object? draftset-uri drafter:hasOwner)))))

  (testing "Other owner"
    (let [draftset-id (create-draftset! *test-backend* test-editor "Test draftset")
          draftset-uri (->draftset-uri draftset-id)]
      (offer-draftset! *test-backend* draftset-id test-publisher :manager)

      (has-string-object? draftset-uri drafter:hasOwner (user/username test-editor))
      (is (= false (has-any-object? draftset-uri drafter:claimableBy))))))

(deftest claim-draftset-test!
  (testing "No owner when user in role"
    (let [draftset-id (create-draftset! *test-backend* test-editor "Test draftset")
          draftset-uri (->draftset-uri draftset-id)]
      (offer-draftset! *test-backend* draftset-id test-editor :publisher)

      (let [err (claim-draftset! *test-backend* draftset-id test-publisher)
            ds-info (get-draftset-info *test-backend* draftset-id)]
        (is (nil? err))
        (is (is-draftset-owner? *test-backend* test-publisher draftset-id))
        (is (= false (has-any-object? draftset-uri drafter:claimableBy))))))

  (testing "Claimed by current owner"
    (let [draftset-id (create-draftset! *test-backend* test-editor)
          err (claim-draftset! *test-backend* draftset-id test-editor)]
      (is (nil? err))
      (is (is-draftset-owner? *test-backend* test-editor draftset-id))))

  (testing "User not in claim role"
    (let [draftset-id (create-draftset! *test-backend* test-editor)]
      (offer-draftset! *test-backend* draftset-id test-editor :manager)
      (let [err (claim-draftset! *test-backend* draftset-id test-publisher)]
        (is (some? err))
        (is (nil? (get-draftset-owner *test-backend* draftset-id))))))

  (testing "Draftset owned by other user"
    (let [draftset-id (create-draftset! *test-backend* test-editor)]
      (let [err (claim-draftset! *test-backend* draftset-id test-publisher)]
        (is (some? err))
        (is (is-draftset-owner? *test-backend* test-editor draftset-id))))))

(use-fixtures :once wrap-db-setup)
(use-fixtures :each wrap-clean-test-db)
