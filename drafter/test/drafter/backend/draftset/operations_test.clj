(ns drafter.backend.draftset.operations-test
  (:require [clojure.test :as t :refer :all]
            [drafter.backend.draftset.draft-management :refer [with-state-graph]]
            [drafter.backend.draftset.operations :as sut]
            [drafter.draftset :refer [->draftset-uri ->DraftsetId ->DraftsetURI]]
            [drafter.rdf.drafter-ontology :refer :all]
            [drafter.rdf.sparql :as sparql]
            [drafter.test-common
             :refer
             [*test-backend*
              ask?
              import-data-to-draft!
              select-all-in-graph
              test-triples
              wrap-system-setup]]
            [drafter.test-helpers.draft-management-helpers :as mgmth]
            [drafter.user :as user]
            [drafter.user-test :refer [test-editor test-manager test-publisher]]
            [grafter-2.rdf4j.io :refer [statements]]
            [grafter-2.rdf4j.repository :as repo]
            [grafter-2.rdf.protocols :refer [->Quad ->Triple context context]]
            [grafter.vocabularies.rdf :refer :all]
            [drafter.test-common :as tc]
            [grafter-2.rdf4j.io :as gio]
            [drafter.rdf.sesame :as ses]
            [drafter.fixture-data :as fd]
            [clojure.java.io :as io]
            [drafter.stasher-test :as stasher-test]
            [grafter-2.rdf.protocols :as pr]
            [drafter.backend.draftset.query-impl :as query-impl]))

(use-fixtures :each
  (wrap-system-setup "test-system.edn" [:drafter.stasher/repo :drafter/write-scheduler])
  tc/with-spec-instrumentation)
;(use-fixtures :each wrap-clean-test-db)

(defn- has-uri-object? [s p uri-o]
  (sparql/eager-query *test-backend* (str "ASK WHERE { <" s "> <" p "> <" uri-o "> }")))

(defn- has-string-object? [s p str-o]
  (sparql/eager-query *test-backend* (str "ASK WHERE { <" s "> <" p "> \"" str-o "\" }")))

(defn- has-any-object? [s p]
  (sparql/eager-query *test-backend* (str "ASK WHERE { <" s "> <" p "> ?o }")))

(defn- assert-user-is-creator-and-owner [user draftset-id]
  (is (has-uri-object? (->draftset-uri draftset-id) drafter:createdBy (user/user->uri user)))
  (is (has-uri-object? (->draftset-uri draftset-id) drafter:hasOwner (user/user->uri user))))

(deftest create-draftset!-test
  (let [title "Test draftset"
        description "Test description"]
    (testing "Without title or description"
      (let [draftset-id (sut/create-draftset! *test-backend* test-editor)
            ds-uri (->draftset-uri draftset-id)]
        (is (sut/draftset-exists? *test-backend* ds-uri))
        (is (= false (has-any-object? ds-uri rdfs:label)))
        (is (= false (has-any-object? ds-uri rdfs:comment)))
        (assert-user-is-creator-and-owner test-editor draftset-id)))

    (testing "Without description"
      (let [draftset-id (sut/create-draftset! *test-backend* test-editor title)
            ds-uri (->draftset-uri draftset-id)]
        (is (has-uri-object? ds-uri rdf:a drafter:DraftSet))
        (is (has-string-object? ds-uri rdfs:label title))
        (is (ask? "<" ds-uri "> <" drafter:createdAt "> ?o"))
        (assert-user-is-creator-and-owner test-editor draftset-id)))

    (testing "With description"
      (let [draftset-id (sut/create-draftset! *test-backend* test-editor title description)
            ds-uri (->draftset-uri draftset-id)]
        (is (has-uri-object? ds-uri rdf:a drafter:DraftSet))
        (is (has-string-object? ds-uri rdfs:label title))
        (is (has-string-object? ds-uri rdfs:comment description))
        (is (ask? "<" ds-uri "> <" drafter:createdAt "> ?o"))
        (assert-user-is-creator-and-owner test-editor draftset-id)))))

(deftest draftset-exists-test
  (testing "Existing draftset"
    (let [draftset-id (sut/create-draftset! *test-backend* test-editor "Test draftset")]
      (is (sut/draftset-exists? *test-backend* draftset-id))))

  (testing "Non-existent draftset"
    (is (= false (sut/draftset-exists? *test-backend* (->DraftsetId "missing"))))))

(deftest get-draftset-owner-test
  (testing "With owner"
    (let [draftset-id (sut/create-draftset! *test-backend* test-editor)
          owner (sut/get-draftset-owner *test-backend* draftset-id)]
      (is (= (user/username test-editor) owner))))

  (testing "With no owner"
    (let [draftset-id (sut/create-draftset! *test-backend* test-editor)]
      (sut/submit-draftset-to-permission! *test-backend*
                                          draftset-id
                                          test-editor
                                          :drafter:draft:claim)
      (let [owner (sut/get-draftset-owner *test-backend* draftset-id)]
        (is (nil? owner))))))

(deftest is-draftset-owner?-test
  (testing "Is owner"
    (let [draftset-id (sut/create-draftset! *test-backend* test-editor)]
      (is (= true (sut/is-draftset-owner? *test-backend* draftset-id test-editor)))))

  (testing "Has no owner"
    (let [draftset-id (sut/create-draftset! *test-backend* test-editor)]
      (sut/submit-draftset-to-permission! *test-backend*
                                          draftset-id
                                          test-editor
                                          :drafter:draft:claim)
      (is (= false (sut/is-draftset-owner? *test-backend* draftset-id test-editor)))))

  (testing "Has different owner"
    (let [draftset-id (sut/create-draftset! *test-backend* test-editor)]
      (is (= false (sut/is-draftset-owner? *test-backend* draftset-id test-publisher))))))

(deftest delete-draftset-statements!-test
  (let [draftset-id (sut/create-draftset! *test-backend* test-editor "Test draftset")]
    (sut/delete-draftset-statements! *test-backend* draftset-id)
    (is (= false (ask? (str "<" (draftset-id->uri draftset-id) ">") "?p" "?o")))))

(defn- import-data-to-draftset! [db draftset-id quads]
  (let [graph-quads (group-by context quads)]
    (doall (map (fn [[live qs]] (import-data-to-draft! db live qs draftset-id)) graph-quads))))

(deftest delete-draftset!-test
  (let [draftset-id (sut/create-draftset! *test-backend* test-editor "Test draftset")
        quads (statements "test/resources/test-draftset.trig")
        draft-graphs (import-data-to-draftset! *test-backend* draftset-id quads)]

    (doseq [dg draft-graphs]
      (is (= true (mgmth/draft-exists? *test-backend* dg))))

    (sut/delete-draftset! *test-backend* draftset-id)

    (is (= false (sut/draftset-exists? *test-backend* draftset-id)))

    (doseq [dg draft-graphs]
      (is (= false (mgmth/draft-exists? *test-backend* dg))))))

(defn- draftset-has-claim-permission? [draftset-id permission]
  (let [q (str
           "ASK WHERE {"
           (with-state-graph
             "<" (->draftset-uri draftset-id) "> <" drafter:hasSubmission "> ?submission ."
             "?submission <" drafter:claimPermission "> \"" (name permission) "\" .")
           "}")]
    (sparql/eager-query *test-backend* q)))

(deftest submit-draftset-to-permission-test!
  (testing "Existing owner"
    (let [draftset-id (sut/create-draftset! *test-backend* test-editor "Test draftset")
          draftset-uri (->draftset-uri draftset-id)]
      (sut/submit-draftset-to-permission! *test-backend*
                                          draftset-id
                                          test-editor
                                          :drafter:draft:claim)

      (is (draftset-has-claim-permission? draftset-id :drafter:draft:claim))
      (is (= false (has-any-object? draftset-uri drafter:hasOwner)))))

  (testing "Submitted by other user"
    (let [draftset-id (sut/create-draftset! *test-backend* test-editor "Test draftset")
          draftset-uri (->draftset-uri draftset-id)]
      (sut/submit-draftset-to-permission! *test-backend*
                                          draftset-id
                                          test-publisher
                                          :drafter:draft:claim)

      (is (sut/is-draftset-owner? *test-backend* draftset-id test-editor))
      (is (= false (has-any-object? draftset-uri drafter:hasSubmission))))))

(defn- draftset-has-claim-user? [draftset-id user]
  (let [q (str
           "ASK WHERE {"
           (with-state-graph
             "<" (->draftset-uri draftset-id) "> <" drafter:hasSubmission "> ?submission ."
             "?submission <" drafter:claimUser "> <" (user/user->uri user) "> .")
           "}")]
    (sparql/eager-query *test-backend* q)))

(defn is-draftset-submitter? [backend draftset-ref user]
  (if-let [{:keys [submitted-by]} (sut/get-draftset-info backend draftset-ref)]
    (= submitted-by (user/username user))
    false))

(deftest submit-draftset-to-user!-test
  (testing "When owned"
    (let [draftset-id (sut/create-draftset! *test-backend* test-editor "Test draftset")]
      (sut/submit-draftset-to-user! *test-backend* draftset-id test-editor test-publisher)

      (is (= nil (sut/get-draftset-owner *test-backend* draftset-id)))
      (is (is-draftset-submitter? *test-backend* draftset-id test-editor))
      (is (draftset-has-claim-user? draftset-id test-publisher))))

  (testing "Submitted to self"
    (let [draftset-id (sut/create-draftset! *test-backend* test-editor)
          draftset-uri (str (->draftset-uri draftset-id))]
      (sut/submit-draftset-to-user! *test-backend* draftset-id test-editor test-editor)

      (sut/is-draftset-owner? *test-backend* draftset-id test-editor)))

  (testing "When not owned"
    (let [draftset-id (sut/create-draftset! *test-backend* test-editor)
          draftset-uri (str (->draftset-uri draftset-id))]

      (sut/submit-draftset-to-user! *test-backend* draftset-id test-manager test-publisher)

      (is (sut/is-draftset-owner? *test-backend* draftset-id test-editor))
      (is (= false (draftset-has-claim-user? draftset-id test-publisher)))
      (is (= false (is-draftset-submitter? *test-backend* draftset-id test-manager)))
      (is (= false (has-uri-object? draftset-uri drafter:submittedBy (user/user->uri test-manager))))))

  (testing "When submitted"
    (let [draftset-id (sut/create-draftset! *test-backend* test-editor)]
      (sut/submit-draftset-to-permission! *test-backend*
                                          draftset-id
                                          test-editor
                                          :drafter:draft:claim)
      (sut/submit-draftset-to-user! *test-backend* draftset-id test-editor test-manager)

      (is (= nil (sut/get-draftset-owner *test-backend* draftset-id)))
      (is (draftset-has-claim-permission? draftset-id :drafter:draft:claim))
      (is (= false (draftset-has-claim-user? draftset-id test-manager))))))

(defn- draftset-has-submission? [draftset-ref]
  (let [draftset-uri (->draftset-uri draftset-ref)
        q (str "ASK WHERE {"
               (with-state-graph
                 "<" draftset-uri "> <" rdf:a "> <" drafter:DraftSet "> ."
                 "<" draftset-uri "> <" drafter:hasSubmission "> ?submission")
               "}")]
    (sparql/eager-query *test-backend* q)))

(deftest claim-draftset-test!
  (testing "No owner when user has claim permission"
    (let [draftset-id (sut/create-draftset! *test-backend* test-editor "Test draftset")
          draftset-uri (->draftset-uri draftset-id)]
      (sut/submit-draftset-to-permission! *test-backend*
                                          draftset-id
                                          test-editor
                                          :drafter:draft:claim)

      (let [[result _] (sut/claim-draftset! *test-backend* draftset-id test-publisher)
            ds-info (sut/get-draftset-info *test-backend* draftset-id)]
        (is (= :ok result))
        (is (sut/is-draftset-owner? *test-backend* draftset-id test-publisher))
        (is (= false (has-any-object? draftset-uri drafter:hasSubmission))))))

  (testing "No owner when submitted to user"
    (let [draftset-id (sut/create-draftset! *test-backend* test-editor "Test draftset")]
      (sut/submit-draftset-to-user! *test-backend* draftset-id test-editor test-publisher)

      (let [[result _] (sut/claim-draftset! *test-backend* draftset-id test-publisher)]
        (is (= :ok result))
        (is (sut/is-draftset-owner? *test-backend* draftset-id test-publisher))
        (is (= false (draftset-has-submission? draftset-id))))))

  (testing "No owner when submitted to different user"
    (let [draftset-id (sut/create-draftset! *test-backend* test-editor "Test draftset")]
      (sut/submit-draftset-to-user! *test-backend* draftset-id test-editor test-publisher)

      (let [[result _] (sut/claim-draftset! *test-backend* draftset-id test-manager)]
        (is (= :forbidden result))
        (is (nil? (sut/get-draftset-owner *test-backend* draftset-id)))
        (is (draftset-has-submission? draftset-id)))))

  (testing "Reclaimed by submitter after submit to permission"
    (let [draftset-id (sut/create-draftset! *test-backend* test-editor "Test draftset")]
      (sut/submit-draftset-to-permission! *test-backend*
                                          draftset-id
                                          test-editor
                                          :drafter:draft:claim)
      (let [[result _] (sut/claim-draftset! *test-backend* draftset-id test-editor)]
        (is (= :ok result))
        (is (sut/is-draftset-owner? *test-backend* draftset-id test-editor))
        (is (= false (draftset-has-submission? draftset-id))))))

  (testing "Reclaimed by submitter after submit to user"
    (let [draftset-id (sut/create-draftset! *test-backend* test-editor "Test draftset")]
      (sut/submit-draftset-to-user! *test-backend* draftset-id test-editor test-publisher)
      (let [[result _] (sut/claim-draftset! *test-backend* draftset-id test-editor)]
        (is (= :ok result))
        (is (sut/is-draftset-owner? *test-backend* draftset-id test-editor))
        (is (= false (draftset-has-submission? draftset-id))))))

  (testing "No owner when user doesn't have permission but is submitter"
    (let [draftset-id (sut/create-draftset! *test-backend* test-editor "Test draftset")]
      (sut/submit-draftset-to-permission! *test-backend*
                                          draftset-id
                                          test-editor
                                          :drafter:draft:publish)
      (let [[result _] (sut/claim-draftset! *test-backend* draftset-id test-editor)]
        (is (= :ok result))
        (is (sut/is-draftset-owner? *test-backend* draftset-id test-editor))
        (is (= false (has-any-object? (->draftset-uri draftset-id) drafter:hasSubmission))))))

  (testing "Owned by other user after submitted by user"
    (let [draftset-id (sut/create-draftset! *test-backend* test-editor "Test draftset")]
      (sut/submit-draftset-to-permission! *test-backend*
                                          draftset-id
                                          test-editor
                                          :drafter:draft:publish)
      (sut/claim-draftset! *test-backend* draftset-id test-publisher)
      (let [[result _] (sut/claim-draftset! *test-backend* draftset-id test-editor)]
        (is (= :forbidden result)))))

  (testing "Claimed by current owner"
    (let [draftset-id (sut/create-draftset! *test-backend* test-editor)
          [result _] (sut/claim-draftset! *test-backend* draftset-id test-editor)]
      (is (= :ok result))
      (is (sut/is-draftset-owner? *test-backend* draftset-id test-editor))))

  (testing "User doesn't have claim permission"
    (let [draftset-id (sut/create-draftset! *test-backend* test-editor)]
      (sut/submit-draftset-to-permission! *test-backend*
                                          draftset-id
                                          test-editor
                                          :drafter:draft:claim:manager)
      (let [[result _] (sut/claim-draftset! *test-backend* draftset-id test-publisher)]
        (is (= :forbidden result))
        (is (nil? (sut/get-draftset-owner *test-backend* draftset-id))))))

  (testing "Draftset owned by other user"
    (let [draftset-id (sut/create-draftset! *test-backend* test-editor)]
      (let [[result _] (sut/claim-draftset! *test-backend* draftset-id test-publisher)]
        (is (= :forbidden result))
        (is (sut/is-draftset-owner? *test-backend* draftset-id test-editor)))))

  (testing "Draftset does not exist"
    (let [[result _] (sut/claim-draftset! *test-backend* (->DraftsetURI "http://missing-draftset") test-publisher)]
      (is (= :not-found result)))))

(t/deftest all-quads-query-test
  (t/testing "Operation methods"
    (let [pquery (sut/all-quads-query *test-backend*)]
      (query-impl/test-operation-methods pquery #{"s" "p" "o" "g"})))

  (t/testing "evaluate"
    (let [pquery (sut/all-quads-query *test-backend*)
          test-data (io/resource "drafter/backend/draftset/operations_test/all_data_queries.trig")
          expected-quads (set (gio/statements test-data))]
      (fd/load-fixture! {:repo     *test-backend*
                         :format   :trig
                         :fixtures [test-data]})
      (t/testing "pull"
        (with-open [results (.evaluate pquery)]
          (let [rs (ses/iteration-seq results)
                quads (map gio/backend-quad->grafter-quad rs)]
            (t/is (= expected-quads (set quads)) "Unexpected results from pull query"))))

      (t/testing "push"
        (let [[events handler] (stasher-test/recording-rdf-handler)]
          (.evaluate pquery handler)
          (t/is (= expected-quads (set (:data @events))) "Unexpected results from push query"))))))

(t/deftest all-graph-triples-query-test
  (let [test-data (io/resource "drafter/backend/draftset/operations_test/all_data_queries.trig")
        quads (gio/statements test-data)
        by-graph (group-by pr/context quads)]
    (fd/load-fixture! {:repo     *test-backend*
                       :format   :trig
                       :fixtures [test-data]})

    (doseq [[graph graph-quads] by-graph]
      (let [pquery (sut/all-graph-triples-query *test-backend* graph)
            graph-triples (set (map pr/map->Triple graph-quads))]
        (t/testing "pull query"
          (with-open [results (.evaluate pquery)]
            (let [rs (ses/iteration-seq results)
                  quads (map gio/backend-quad->grafter-quad rs)]
              (t/is (= graph-triples (set quads)) "Unexpected results from pull query"))))

        (t/testing "push query"
          (let [[events handler] (stasher-test/recording-rdf-handler)]
            (.evaluate pquery handler)
            (t/is (= graph-triples (set (:data @events))) "Unexpected results from push query")))))))
