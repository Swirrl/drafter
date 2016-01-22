(ns drafter.rdf.draftset-management-test
  (:require [clojure.test :refer :all]
            [drafter.rdf.draftset-management :refer :all]
            [drafter.rdf.draft-management :refer [draft-exists?] :as mgmt]
            [drafter.test-common :refer [*test-backend* wrap-db-setup wrap-clean-test-db ask? import-data-to-draft!]]
            [grafter.rdf :refer [statements context]]
            [drafter.rdf.drafter-ontology :refer :all :as ont]
            [grafter.rdf.repository :refer [query]]
            [grafter.vocabularies.rdf :refer :all]))

(defn- has-uri-object? [s p uri-o]
  (query *test-backend* (str "ASK WHERE { <" s "> <" p "> <" uri-o "> }")))

(defn- has-string-object? [s p str-o]
  (query *test-backend* (str "ASK WHERE { <" s "> <" p "> \"" str-o "\" }")))

(defn- has-any-object? [s p]
  (query *test-backend* (str "ASK WHERE { <" s "> <" p "> ?o }")))

(deftest create-draftset!-test
  (let [title "Test draftset"
        description "Test description"]
    (testing "Without title or description"
      (let [draftset-id (create-draftset! *test-backend*)
            ds-uri (->draftset-uri draftset-id)]
        (is (draftset-exists? *test-backend* ds-uri))
        (is (= false (has-any-object? ds-uri rdfs:label)))
        (is (= false (has-any-object? ds-uri rdfs:comment)))))

    (testing "Without description"
      (let [draftset-id (create-draftset! *test-backend* title)
            ds-uri (->draftset-uri draftset-id)]
        (is (has-uri-object? ds-uri rdf:a drafter:DraftSet))
        (is (has-string-object? ds-uri rdfs:label title))
        (is (ask? "<" ds-uri "> <" drafter:createdAt "> ?o"))))

    (testing "With description"
      (let [draftset-id (create-draftset! *test-backend* title description)
            ds-uri (->draftset-uri draftset-id)]
        (is (has-uri-object? ds-uri rdf:a drafter:DraftSet))
        (is (has-string-object? ds-uri rdfs:label title))
        (is (has-string-object? ds-uri rdfs:comment description))
        (is (ask? "<" ds-uri "> <" drafter:createdAt "> ?o"))))))

(deftest draftset-exists-test
  (testing "Existing draftset"
    (let [draftset-id (create-draftset! *test-backend* "Test draftset")]
      (is (draftset-exists? *test-backend* draftset-id))))

  (testing "Non-existent draftset"
    (is (= false (draftset-exists? *test-backend* (->DraftsetId "missing"))))))

(deftest delete-draftset-statements!-test
  (let [draftset-id (create-draftset! *test-backend* "Test draftset")]
    (delete-draftset-statements! *test-backend* draftset-id)
    (is (= false (ask? (str "<" draftset-uri ">") "?p" "?o")))))

(defn- import-data-to-draftset! [db draftset-id quads]
  (let [graph-quads (group-by context quads)]
    (doall (map (fn [[live qs]] (import-data-to-draft! *test-backend* live qs draftset-id)) graph-quads))))

(deftest delete-draftset!-test
  (let [draftset-id (create-draftset! *test-backend* "Test draftset")
        quads (statements "test/resources/test-draftset.trig")
        draft-graphs (import-data-to-draftset! *test-backend* draftset-id quads)]
    
    (doseq [dg draft-graphs]
      (is (= true (draft-exists? *test-backend* dg))))

    (delete-draftset! *test-backend* draftset-id)

    (is (= false (draftset-exists? *test-backend* draftset-id)))

    (doseq [dg draft-graphs]
      (is (= false (draft-exists? *test-backend* dg))))))

(deftest delete-draftest-graph!-test
  (testing "Draftset graph"
    (let [draftset-id (create-draftset! *test-backend* "Test draftset")
          quads (statements "test/resources/test-draftset.trig")]
      
      (import-data-to-draftset! *test-backend* draftset-id quads)
      
      (let [[live draft] (first (get-draftset-graph-mapping *test-backend* draftset-id))]
        (delete-draftset-graph! *test-backend* draftset-id live)
        (is (= false (draft-exists? *test-backend* draft))))))

  (testing "Unknown graph"
    (let [draftset-id (create-draftset! *test-backend* "Test draftset")]
      (import-data-to-draftset! *test-backend* draftset-id (statements "test/resources/test-draftset.trig"))

      (let [old-mapping (get-draftset-graph-mapping *test-backend* draftset-id)]
        (delete-draftset-graph! *test-backend* draftset-id (mgmt/make-draft-graph-uri))

        (is (= old-mapping (get-draftset-graph-mapping *test-backend* draftset-id)))))))

(use-fixtures :once wrap-db-setup)
(use-fixtures :each wrap-clean-test-db)
