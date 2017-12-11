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
            [grafter.rdf :refer [context statements triple=]]
            [grafter.rdf.protocols :refer [->Quad ->Triple]]
            [grafter.url :as url]
            [grafter.vocabularies.rdf :refer :all])
  (:import java.net.URI
           org.eclipse.rdf4j.rio.RDFFormat))

(use-fixtures :each (wrap-system-setup "test-system.edn" [:drafter.backend/rdf4j-repo :drafter/write-scheduler]))
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

(deftest delete-draftest-graph!-test
  (testing "Delete non-existent live graph"
    (let [draftset-id (sut/create-draftset! *test-backend* test-editor "Test draftset")
          graph-to-delete (URI. "http://missing")]
      (sut/delete-draftset-graph! *test-backend* draftset-id graph-to-delete)

      (is (= false (mgmt/is-graph-managed? *test-backend* graph-to-delete)))
      (is (empty? (sut/get-draftset-graph-mapping *test-backend* draftset-id)))))

  (testing "Delete live graph not already in draftset"
    (let [live-graph (URI. "http://live")
          draftset-id (sut/create-draftset! *test-backend* test-editor "Test draftset")]
      (make-graph-live! *test-backend* live-graph)
      (sut/delete-draftset-graph! *test-backend* draftset-id live-graph)

      (is (mgmt/is-graph-managed? *test-backend* live-graph))

      (let [graph-mapping (sut/get-draftset-graph-mapping *test-backend* draftset-id)]
        (is (contains? graph-mapping live-graph))

        (is (mgmth/draft-exists? *test-backend* (get graph-mapping live-graph))))))

  (testing "Graph already in draftset"
    (let [live-graph (URI. "http://live")
          draftset-id (sut/create-draftset! *test-backend* test-editor "Test draftset")]
      (make-graph-live! *test-backend* live-graph)

      (let [draft-graph (import-data-to-draft! *test-backend* live-graph (test-triples (URI. "http://subject")) draftset-id)]
        (is (mgmth/draft-exists? *test-backend* draft-graph))

        (sut/delete-draftset-graph! *test-backend* draftset-id live-graph)

        (is (mgmth/draft-exists? *test-backend* draft-graph))
        (is (= false (ask? (format "GRAPH <%s> { ?s ?p ?o }" draft-graph))))))))

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

(deftest revert-changes-from-graph-only-in-draftset
  (let [live-graph (URI. "http://live")
        draftset-id (sut/create-draftset! *test-backend* test-editor)]
    (mgmt/create-managed-graph! *test-backend* live-graph)
    (let [draft-graph (mgmt/create-draft-graph! *test-backend* live-graph draftset-id)
          result (sut/revert-graph-changes! *test-backend* draftset-id live-graph)]
      (is (= :reverted result))
      (is (= false (mgmth/draft-exists? *test-backend* draft-graph)))
      (is (= false (mgmt/is-graph-managed? *test-backend* live-graph))))))

(deftest revert-changes-from-graph-which-exists-in-live
  (let [live-graph-uri (make-graph-live! *test-backend* (URI. "http://live"))
        draftset-id (sut/create-draftset! *test-backend* test-editor)
        draft-graph-uri (sut/delete-draftset-graph! *test-backend* draftset-id live-graph-uri)]
    (let [result (sut/revert-graph-changes! *test-backend* draftset-id live-graph-uri)]
      (is (= :reverted result))
      (is (mgmt/is-graph-managed? *test-backend* live-graph-uri))
      (is (= false (mgmth/draft-exists? *test-backend* draft-graph-uri))))))

(deftest revert-change-from-graph-which-exists-independently-in-other-draftset
  (let [live-graph-uri (mgmt/create-managed-graph! *test-backend* (URI. "http://live"))
        ds1-id (sut/create-draftset! *test-backend* test-editor)
        ds2-id (sut/create-draftset! *test-backend* test-publisher)
        draft-graph1-uri (mgmt/create-draft-graph! *test-backend* live-graph-uri ds1-id)
        draft-graph2-uri (mgmt/create-draft-graph! *test-backend* live-graph-uri ds2-id)]

    (let [result (sut/revert-graph-changes! *test-backend* ds2-id live-graph-uri)]
      (is (= :reverted result))
      (is (mgmt/is-graph-managed? *test-backend* live-graph-uri))
      (is (= false (mgmth/draft-exists? *test-backend* draft-graph2-uri)))
      (is (mgmth/draft-exists? *test-backend* draft-graph1-uri)))))

(deftest revert-non-existent-change-in-draftset
  (let [draftset-id (sut/create-draftset! *test-backend* test-editor)
        result (sut/revert-graph-changes! *test-backend* draftset-id (URI. "http://missing"))]
    (is (= :not-found result))))

(deftest revert-changes-in-non-existent-draftset
  (let [live-graph (make-graph-live! *test-backend* (URI. "http://live"))
        result (sut/revert-graph-changes! *test-backend* (->DraftsetId "missing") live-graph)]
    (is (= :not-found result))))

(deftest quad-batch->graph-triples-test
  (testing "Batch quads have nil graph"
    (let [quads [(->Quad (URI. "http://s1") (URI. "http://p1") "o1" nil)
                 (->Quad (URI. "http://s2") (URI. "http://p2") "o2" nil)]]
      (is (thrown? IllegalArgumentException (sut/quad-batch->graph-triples quads)))))

  (testing "Non-empty batch"
    (let [guri "http://graph"
          quads (map #(->Quad (str "http://s" %) (str "http://p" %) (str "http://o" %) guri) (range 1 10))
          {:keys [graph-uri triples]} (sut/quad-batch->graph-triples quads)]
      (is (= guri graph-uri))
      (is (every? identity (map triple= quads triples))))))