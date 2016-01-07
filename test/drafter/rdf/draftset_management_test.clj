(ns drafter.rdf.draftset-management-test
  (:require [clojure.test :refer :all]
            [drafter.rdf.draftset-management :refer :all]
            [drafter.test-common :refer [*test-backend* wrap-db-setup wrap-clean-test-db ask?]]
            [drafter.rdf.drafter-ontology :refer :all :as ont]
            [grafter.rdf.repository :refer [query]]
            [grafter.vocabularies.rdf :refer :all]))

(defn- has-uri-object? [s p uri-o]
  (query *test-backend* (str "ASK WHERE { <" s "> <" p "> <" uri-o "> }")))

(defn- has-string-object? [s p str-o]
  (query *test-backend* (str "ASK WHERE { <" s "> <" p "> \"" str-o "\" }")))

(deftest create-draftset!-test
  (let [title "Test draftset"
        description "Test description"]
    (testing "Without description"
      (let [draftset-id (create-draftset! *test-backend* title)
            ds-uri (ont/draftset-uri draftset-id)]
        (is (has-uri-object? ds-uri rdf:a drafter:DraftSet))
        (is (has-string-object? ds-uri rdfs:label title))
        (is (ask? "<" ds-uri "> <" drafter:createdAt "> ?o"))))

    (testing "With description"
      (let [draftset-id (create-draftset! *test-backend* title description)
            ds-uri (draftset-uri draftset-id)]
        (is (has-uri-object? ds-uri rdf:a drafter:DraftSet))
        (is (has-string-object? ds-uri rdfs:label title))
        (is (has-string-object? ds-uri rdfs:comment description))
        (is (ask? "<" ds-uri "> <" drafter:createdAt "> ?o"))))))

(deftest draftset-exists-test
  (testing "Existing draftset"
    (let [draftset-id (create-draftset! *test-backend* "Test draftset")]
      (is (draftset-exists? *test-backend* draftset-id))))

  (testing "Non-existent draftset"
    (is (= false (draftset-exists? *test-backend* "missing")))))

(deftest delete-draftset-statements!-test
  (let [draftset-id (create-draftset! *test-backend* "Test draftset")
        draftset-uri (ont/draftset-uri draftset-id)]
    (delete-draftset-statements! *test-backend* draftset-uri)
    (is (= false (ask? (str "<" draftset-uri ">") "?p" "?o")))))

(use-fixtures :once wrap-db-setup)
(use-fixtures :each wrap-clean-test-db)
