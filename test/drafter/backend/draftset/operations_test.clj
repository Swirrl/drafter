(ns drafter.backend.draftset.operations-test
  (:require [clojure.java.io :as io]
            [clojure.test :refer :all]
            [drafter.backend.draftset.draft-management
             :as
             mgmt
             :refer
             [with-state-graph]]
            [drafter.backend.draftset.operations :as sut]
            [drafter.draftset :refer [->draftset-uri ->DraftsetId ->DraftsetURI]]
            [drafter.rdf.drafter-ontology :refer :all]
            [drafter.rdf.sparql :as sparql]
            [drafter.test-common
             :refer
             [*test-backend*
              ask?
              import-data-to-draft!
              make-graph-live!
              select-all-in-graph
              test-triples
              wrap-system-setup]]
            [drafter.test-helpers.draft-management-helpers :as mgmth]
            [drafter.user :as user]
            [drafter.user-test :refer [test-editor test-manager test-publisher]]
            [drafter.util :as util]
            [drafter.write-scheduler :as scheduler]
            [grafter-2.rdf4j.io :refer [statements]]
            [grafter-2.rdf4j.repository :as repo]
            [grafter-2.rdf.protocols :refer [->Quad ->Triple context context]]
            [grafter.url :as url]
            [grafter.vocabularies.rdf :refer :all])
  (:import java.net.URI
           org.eclipse.rdf4j.rio.RDFFormat))

(use-fixtures :each (wrap-system-setup "test-system.edn" [:drafter.stasher/repo :drafter/write-scheduler]))
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
      (sut/submit-draftset-to-role! *test-backend* draftset-id test-editor :publisher)
      (let [owner (sut/get-draftset-owner *test-backend* draftset-id)]
        (is (nil? owner))))))

(deftest is-draftset-owner?-test
  (testing "Is owner"
    (let [draftset-id (sut/create-draftset! *test-backend* test-editor)]
      (is (= true (sut/is-draftset-owner? *test-backend* draftset-id test-editor)))))

  (testing "Has no owner"
    (let [draftset-id (sut/create-draftset! *test-backend* test-editor)]
      (sut/submit-draftset-to-role! *test-backend* draftset-id test-editor :publisher)
      (is (= false (sut/is-draftset-owner? *test-backend* draftset-id test-editor)))))

  (testing "Has different owner"
    (let [draftset-id (sut/create-draftset! *test-backend* test-editor)]
      (is (= false (sut/is-draftset-owner? *test-backend* draftset-id test-publisher))))))

(deftest delete-draftset-statements!-test
  (let [draftset-id (sut/create-draftset! *test-backend* test-editor "Test draftset")]
    (sut/delete-draftset-statements! *test-backend* draftset-id)
    (is (= false (ask? (str "<" (draftset-id->uri draftset-id) ">") "?p" "?o")))))

(defn- import-data-to-draftset! [db draftset-id quads clock-fn]
  (let [graph-quads (group-by context quads)]
    (doall (map (fn [[live qs]] (import-data-to-draft! db live qs draftset-id clock-fn)) graph-quads))))

(deftest delete-draftset!-test
  (let [draftset-id (sut/create-draftset! *test-backend* test-editor "Test draftset")
        quads (statements "test/resources/test-draftset.trig")
        draft-graphs (import-data-to-draftset! *test-backend* draftset-id quads (constantly #inst "2018"))]

    (doseq [dg draft-graphs]
      (is (= true (mgmth/draft-exists? *test-backend* dg))))

    (sut/delete-draftset! *test-backend* draftset-id)

    (is (= false (sut/draftset-exists? *test-backend* draftset-id)))

    (doseq [dg draft-graphs]
      (is (= false (mgmth/draft-exists? *test-backend* dg))))))

(deftest delete-draftest-graph!-test-1
  (let [initial-time-fn (fn [] #inst "2017")
        modified-time-fn (fn [] #inst "2018")
        id-fn (constantly #uuid "00000000-0000-0000-0000-000000000000")
        create-test-draftset! #(sut/create-draftset! *test-backend* test-editor "Test draftset" nil id-fn modified-time-fn)]
    (testing "Delete non-existent live graph"
      (let [draftset-id (create-test-draftset!)
            graph-to-delete (URI. "http://missing")]

        (sut/delete-draftset-graph! *test-backend* draftset-id graph-to-delete modified-time-fn)

        (is (= false (mgmt/is-graph-managed? *test-backend* graph-to-delete)))
        (is (empty? (sut/get-draftset-graph-mapping *test-backend* draftset-id)))))))

(deftest delete-draftset-graph!-test-2
  (let [initial-time-fn (fn [] #inst "2017")
        modified-time-fn (fn [] #inst "2018")
        id-fn (constantly #uuid "00000000-0000-0000-0000-000000000000")
        create-test-draftset! #(sut/create-draftset! *test-backend* test-editor "Test draftset" nil id-fn modified-time-fn)]
    (testing "Delete live graph not already in draftset"
      (let [live-graph (URI. "http://live")
            draftset-id (create-test-draftset!)]
        (make-graph-live! *test-backend* live-graph initial-time-fn)
        (sut/delete-draftset-graph! *test-backend* draftset-id live-graph modified-time-fn)

        (is (mgmt/is-graph-managed? *test-backend* live-graph))

        (let [graph-mapping (sut/get-draftset-graph-mapping *test-backend* draftset-id)]
          (is (contains? graph-mapping live-graph))

          (is (mgmth/draft-exists? *test-backend* (get graph-mapping live-graph))))))))

(deftest delete-draftset-graph!-test-3
  (let [initial-time-fn (fn [] #inst "2017")
        modified-time-fn (fn [] #inst "2019")
        id-fn (constantly #uuid "00000000-0000-0000-0000-000000000000")
        create-test-draftset! #(sut/create-draftset! *test-backend* test-editor "Test draftset" nil id-fn initial-time-fn)]
    (testing "Graph already in draftset"
      (let [live-graph (URI. "http://live")
            draftset-id (create-test-draftset!)]

        (make-graph-live! *test-backend* live-graph initial-time-fn) ;; note this isn't actual a publish operation it's just setting up stategraph state

        (let [draft-graph (import-data-to-draft! *test-backend* live-graph (test-triples (URI. "http://subject")) draftset-id initial-time-fn)
              fetch-modified (fn [repo graph-uri] (:modified (first (repo/query (repo/->connection repo) (format "SELECT ?modified WHERE { <%s> <http://purl.org/dc/terms/modified> ?modified .}" graph-uri)))))
              initially-modified-at (fetch-modified *test-backend* live-graph)]

          (is (mgmth/draft-exists? *test-backend* draft-graph))

          (sut/delete-draftset-graph! *test-backend* draftset-id live-graph modified-time-fn)

          (let [subsequently-modified-at (fetch-modified (repo/->connection *test-backend*) draft-graph)]
            (is (.isBefore initially-modified-at
                           subsequently-modified-at)
                "Modified time is updated"))

          (is (mgmth/draft-exists? *test-backend* draft-graph))
          (is (= false (ask? (format "GRAPH <%s> { ?s ?p ?o }" draft-graph)))))))))

(defn- draftset-has-claim-role? [draftset-id role]
  (let [q (str
           "ASK WHERE {"
           (with-state-graph
             "<" (->draftset-uri draftset-id) "> <" drafter:hasSubmission "> ?submission ."
             "?submission <" drafter:claimRole "> \"" (name role) "\" .")
           "}")]
    (sparql/eager-query *test-backend* q)))

(deftest submit-draftset-to-role-test!
  (testing "Existing owner"
    (let [draftset-id (sut/create-draftset! *test-backend* test-editor "Test draftset")
          draftset-uri (->draftset-uri draftset-id)]
      (sut/submit-draftset-to-role! *test-backend* draftset-id test-editor :publisher)

      (is (draftset-has-claim-role? draftset-id :publisher))
      (is (= false (has-any-object? draftset-uri drafter:hasOwner)))))

  (testing "Submitted by other user"
    (let [draftset-id (sut/create-draftset! *test-backend* test-editor "Test draftset")
          draftset-uri (->draftset-uri draftset-id)]
      (sut/submit-draftset-to-role! *test-backend* draftset-id test-publisher :manager)

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
      (sut/submit-draftset-to-role! *test-backend* draftset-id test-editor :publisher)
      (sut/submit-draftset-to-user! *test-backend* draftset-id test-editor test-manager)

      (is (= nil (sut/get-draftset-owner *test-backend* draftset-id)))
      (is (draftset-has-claim-role? draftset-id :publisher))
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
  (testing "No owner when user in role"
    (let [draftset-id (sut/create-draftset! *test-backend* test-editor "Test draftset")
          draftset-uri (->draftset-uri draftset-id)]
      (sut/submit-draftset-to-role! *test-backend* draftset-id test-editor :publisher)

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
        (is (= :user result))
        (is (nil? (sut/get-draftset-owner *test-backend* draftset-id)))
        (is (draftset-has-submission? draftset-id)))))

  (testing "Reclaimed by submitter after submit to role"
    (let [draftset-id (sut/create-draftset! *test-backend* test-editor "Test draftset")]
      (sut/submit-draftset-to-role! *test-backend* draftset-id test-editor :publisher)
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

  (testing "No owner when user is submitter not in role"
    (let [draftset-id (sut/create-draftset! *test-backend* test-editor "Test draftset")]
      (sut/submit-draftset-to-role! *test-backend* draftset-id test-editor :publisher)
      (let [[result _] (sut/claim-draftset! *test-backend* draftset-id test-editor)]
        (is (= :ok result))
        (is (sut/is-draftset-owner? *test-backend* draftset-id test-editor))
        (is (= false (has-any-object? (->draftset-uri draftset-id) drafter:hasSubmission))))))

  (testing "Owned by other user after submitted by user"
    (let [draftset-id (sut/create-draftset! *test-backend* test-editor "Test draftset")]
      (sut/submit-draftset-to-role! *test-backend* draftset-id test-editor :publisher)
      (sut/claim-draftset! *test-backend* draftset-id test-publisher)
      (let [[result _] (sut/claim-draftset! *test-backend* draftset-id test-editor)]
        (is (= :owned result)))))

  (testing "Claimed by current owner"
    (let [draftset-id (sut/create-draftset! *test-backend* test-editor)
          [result _] (sut/claim-draftset! *test-backend* draftset-id test-editor)]
      (is (= :ok result))
      (is (sut/is-draftset-owner? *test-backend* draftset-id test-editor))))

  (testing "User not in claim role"
    (let [draftset-id (sut/create-draftset! *test-backend* test-editor)]
      (sut/submit-draftset-to-role! *test-backend* draftset-id test-editor :manager)
      (let [[result _] (sut/claim-draftset! *test-backend* draftset-id test-publisher)]
        (is (= :role result))
        (is (nil? (sut/get-draftset-owner *test-backend* draftset-id))))))

  (testing "Draftset owned by other user"
    (let [draftset-id (sut/create-draftset! *test-backend* test-editor)]
      (let [[result _] (sut/claim-draftset! *test-backend* draftset-id test-publisher)]
        (is (= :owned result))
        (is (sut/is-draftset-owner? *test-backend* draftset-id test-editor)))))

  (testing "Draftset does not exist"
    (let [[result _] (sut/claim-draftset! *test-backend* (->DraftsetURI "http://missing-draftset") test-publisher)]
      (is (= :not-found result)))))
