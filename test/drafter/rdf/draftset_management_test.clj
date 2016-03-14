(ns drafter.rdf.draftset-management-test
  (:require [clojure.test :refer :all]
            [drafter.rdf.draftset-management :refer :all]
            [drafter.rdf.draft-management :refer [draft-exists?] :as mgmt]
            [drafter.test-common :refer [*test-backend* wrap-db-setup wrap-clean-test-db ask? import-data-to-draft! make-graph-live! test-triples
                                         select-all-in-graph]]
            [drafter.write-scheduler :as scheduler]
            [grafter.rdf :refer [statements context]]
            [drafter.rdf.drafter-ontology :refer :all :as ont]
            [drafter.user :as user]
            [drafter.user-test :refer [test-editor test-publisher]]
            [drafter.draftset :refer [->DraftsetId ->DraftsetURI ->draftset-uri]]
            [drafter.util :as util]
            [grafter.rdf.repository :refer [query]]
            [grafter.rdf.protocols :refer [->Triple]]
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
      (submit-draftset! *test-backend* draftset-id test-editor :publisher)
      (let [owner (get-draftset-owner *test-backend* draftset-id)]
        (is (nil? owner))))))

(deftest is-draftset-owner?-test
  (testing "Is owner"
    (let [draftset-id (create-draftset! *test-backend* test-editor)]
      (is (= true (is-draftset-owner? *test-backend* draftset-id test-editor)))))

  (testing "Has no owner"
    (let [draftset-id (create-draftset! *test-backend* test-editor)]
      (submit-draftset! *test-backend* draftset-id test-editor :publisher)
      (is (= false (is-draftset-owner? *test-backend* draftset-id test-editor)))))

  (testing "Has different owner"
    (let [draftset-id (create-draftset! *test-backend* test-editor)]
      (is (= false (is-draftset-owner? *test-backend* draftset-id test-publisher))))))

(deftest delete-draftset-statements!-test
  (let [draftset-id (create-draftset! *test-backend* test-editor "Test draftset")]
    (delete-draftset-statements! *test-backend* draftset-id)
    (is (= false (ask? (str "<" (draftset-uri draftset-id) ">") "?p" "?o")))))

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

(deftest submit-draftset-test!
  (testing "Existing owner"
    (let [draftset-id (create-draftset! *test-backend* test-editor "Test draftset")
          draftset-uri (->draftset-uri draftset-id)]
      (submit-draftset! *test-backend* draftset-id test-editor :publisher)

      (has-string-object? draftset-uri drafter:claimableBy "publisher")
      (is (= false (has-any-object? draftset-uri drafter:hasOwner)))))

  (testing "Other owner"
    (let [draftset-id (create-draftset! *test-backend* test-editor "Test draftset")
          draftset-uri (->draftset-uri draftset-id)]
      (submit-draftset! *test-backend* draftset-id test-publisher :manager)

      (has-string-object? draftset-uri drafter:hasOwner (user/username test-editor))
      (is (= false (has-any-object? draftset-uri drafter:claimableBy))))))

(deftest claim-draftset-test!
  (testing "No owner when user in role"
    (let [draftset-id (create-draftset! *test-backend* test-editor "Test draftset")
          draftset-uri (->draftset-uri draftset-id)]
      (submit-draftset! *test-backend* draftset-id test-editor :publisher)

      (let [[result _] (claim-draftset! *test-backend* draftset-id test-publisher)
            ds-info (get-draftset-info *test-backend* draftset-id)]
        (is (= :ok result))
        (is (is-draftset-owner? *test-backend* draftset-id test-publisher))
        (is (= false (has-any-object? draftset-uri drafter:claimableBy))))))

  (testing "Claimed by current owner"
    (let [draftset-id (create-draftset! *test-backend* test-editor)
          [result _] (claim-draftset! *test-backend* draftset-id test-editor)]
      (is (= :ok result))
      (is (is-draftset-owner? *test-backend* draftset-id test-editor))))

  (testing "User not in claim role"
    (let [draftset-id (create-draftset! *test-backend* test-editor)]
      (submit-draftset! *test-backend* draftset-id test-editor :manager)
      (let [[result _] (claim-draftset! *test-backend* draftset-id test-publisher)]
        (is (= :role result))
        (is (nil? (get-draftset-owner *test-backend* draftset-id))))))

  (testing "Draftset owned by other user"
    (let [draftset-id (create-draftset! *test-backend* test-editor)]
      (let [[result _] (claim-draftset! *test-backend* draftset-id test-publisher)]
        (is (= :owned result))
        (is (is-draftset-owner? *test-backend* draftset-id test-editor)))))

  (testing "Draftset does not exist"
    (let [[result _] (claim-draftset! *test-backend* (->DraftsetURI "http://missing-draftset") test-publisher)]
      (is (= :not-found result)))))

(deftest return-draftset!-test
  (let [draftset-id (create-draftset! *test-backend* test-editor)]
    (submit-draftset! *test-backend* draftset-id test-editor :publisher)
    (claim-draftset! *test-backend* draftset-id test-publisher)
    (return-draftset! *test-backend* draftset-id)

    (is (is-draftset-owner? *test-backend* draftset-id test-editor))))

(deftest revert-changes-from-graph-only-in-draftset
  (let [live-graph "http://live"
        draftset-id (create-draftset! *test-backend* test-editor)]
    (mgmt/create-managed-graph! *test-backend* live-graph)
    (let [draft-graph (mgmt/create-draft-graph! *test-backend* live-graph {} (str (->draftset-uri draftset-id)))
          result (revert-graph-changes! *test-backend* draftset-id live-graph)]
      (is (= :reverted result))
      (is (= false (mgmt/draft-exists? *test-backend* draft-graph)))
      (is (= false (mgmt/is-graph-managed? *test-backend* live-graph))))))

(deftest revert-changes-from-graph-which-exists-in-live
  (let [live-graph-uri (make-graph-live! *test-backend* "http://live")
        draftset-id (create-draftset! *test-backend* test-editor)
        draftset-uri (str (->draftset-uri draftset-id))
        draft-graph-uri (delete-draftset-graph! *test-backend* draftset-id live-graph-uri)]
    (let [result (revert-graph-changes! *test-backend* draftset-id live-graph-uri)]
      (is (= :reverted result))
      (is (mgmt/is-graph-managed? *test-backend* live-graph-uri))
      (is (= false (mgmt/draft-exists? *test-backend* draft-graph-uri))))))

(deftest revert-change-from-graph-which-exists-independently-in-other-draftset
  (let [live-graph-uri (mgmt/create-managed-graph! *test-backend* "http://live")
        ds1-id (create-draftset! *test-backend* test-editor)
        ds2-id (create-draftset! *test-backend* test-publisher)
        draft-graph1-uri (mgmt/create-draft-graph! *test-backend* live-graph-uri {} (str (->draftset-uri ds1-id)))
        draft-graph2-uri (mgmt/create-draft-graph! *test-backend* live-graph-uri {} (str (->draftset-uri ds2-id)))]

    (let [result (revert-graph-changes! *test-backend* ds2-id live-graph-uri)]
      (is (= :reverted result))
      (is (mgmt/is-graph-managed? *test-backend* live-graph-uri))
      (is (= false (draft-exists? *test-backend* draft-graph2-uri)))
      (is (draft-exists? *test-backend* draft-graph1-uri)))))

(deftest revert-non-existent-change-in-draftset
  (let [draftset-id (create-draftset! *test-backend* test-editor)
        result (revert-graph-changes! *test-backend* draftset-id "http://missing")]
    (is (= :not-found result))))

(deftest revert-changes-in-non-existent-draftset
  (let [live-graph (make-graph-live! *test-backend* "http://live")
        result (revert-graph-changes! *test-backend* (->DraftsetId "missing") live-graph)]
    (is (= :not-found result))))

(defn- get-graph-triples [graph-uri]
  (let [results (query *test-backend* (select-all-in-graph graph-uri))]
    (map (fn [{:strs [s p o]}] (->Triple (str s) (str p) (str o))) results)))

(deftest copy-live-graph-into-draftset-test
  (let [draftset-id (create-draftset! *test-backend* test-editor)
        live-triples (test-triples "http://test-subject")
        live-graph-uri (make-graph-live! *test-backend* "http://live" live-triples)
        {:keys [value-p] :as copy-job} (copy-live-graph-into-draftset-job *test-backend* draftset-id live-graph-uri)]
    (scheduler/queue-job! copy-job)

    @value-p

    (let [draft-graph (find-draftset-draft-graph *test-backend* draftset-id live-graph-uri)
          draft-triples (get-graph-triples draft-graph)]
      (is (= (set live-triples) (set draft-triples))))))

(deftest copy-live-graph-into-existing-draft-graph-in-draftset-test
  (let [draftset-id (create-draftset! *test-backend* test-editor)
        live-triples (test-triples "http://test-subject")
        live-graph-uri (make-graph-live! *test-backend* "http://live" live-triples)
        initial-draft-triples (test-triples "http://temp-subject")
        draft-graph-uri (import-data-to-draft! *test-backend* live-graph-uri initial-draft-triples draftset-id)
        {:keys [value-p] :as copy-job} (copy-live-graph-into-draftset-job *test-backend* draftset-id live-graph-uri)]

    (scheduler/queue-job! copy-job)
    @value-p

    (let [draft-triples (get-graph-triples draft-graph-uri)]
      (is (= (set live-triples) (set draft-triples))))))

(deftest set-timestamp-test
  (let [draftset (create-draftset! *test-backend* test-editor)
        triples (test-triples "http://test-subject")
        draft-graph-uri (import-data-to-draft! *test-backend* "http://foo/graph" triples draftset)]

    (set-timestamp-on-draft-graph! *test-backend* draft-graph-uri drafter:createdAt)
    (set-timestamp-on-draft-graph! *test-backend* draft-graph-uri drafter:modifiedAt)

    (is (query *test-backend*
               (str
                "ASK {"
                "<" draft-graph-uri "> <" drafter:createdAt "> ?created . "
                "<" draft-graph-uri "> <" drafter:modifiedAt "> ?modified . "
                "}")))))

(use-fixtures :once wrap-db-setup)
(use-fixtures :each wrap-clean-test-db)
