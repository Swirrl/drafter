(ns drafter.rdf.draftset-management-test
  (:require [clojure.java.io :as io]
            [clojure.test :refer :all]
            [drafter
             [draftset :refer [->draftset-uri ->DraftsetId ->DraftsetURI]]
             [test-common :refer [*test-backend* ask? import-data-to-draft! make-graph-live! select-all-in-graph test-triples wrap-clean-test-db wrap-db-setup]]
             [user :as user]
             [user-test :refer [test-editor test-manager test-publisher]]
             [write-scheduler :as scheduler]]
            [drafter.rdf
             [draft-management :as mgmt :refer [with-state-graph]]
             [drafter-ontology :refer :all]
             [draftset-management :refer :all]]
            [drafter.test-helpers.draft-management-helpers :as mgmth]
            [drafter.test-generators :as gen]
            [grafter.rdf :refer [context statements triple=]]
            [grafter.rdf
             [protocols :refer [->Quad ->Triple]]
             [repository :refer [query]]]
            [grafter.vocabularies.rdf :refer :all]
            [drafter.util :as util]
            [grafter.rdf.repository :as repo])
  (:import org.openrdf.rio.RDFFormat
           (java.net URI)
           (java.util UUID)))

(defn- has-uri-object? [s p uri-o]
  (query *test-backend* (str "ASK WHERE { <" s "> <" p "> <" uri-o "> }")))

(defn- has-string-object? [s p str-o]
  (query *test-backend* (str "ASK WHERE { <" s "> <" p "> \"" str-o "\" }")))

(defn- has-any-object? [s p]
  (query *test-backend* (str "ASK WHERE { <" s "> <" p "> ?o }")))

(defn- assert-user-is-creator-and-owner [user draftset-id]
  (is (has-uri-object? (->draftset-uri draftset-id) drafter:createdBy (user/user->uri user)))
  (is (has-uri-object? (->draftset-uri draftset-id) drafter:hasOwner (user/user->uri user))))

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
    (let [draftset-id (->DraftsetId (str (UUID/randomUUID)))]
      (gen/generate-in *test-backend* {:draftsets [{:id draftset-id}]})
      (is (draftset-exists? *test-backend* draftset-id))))

  (testing "Non-existent draftset"
    (is (= false (draftset-exists? *test-backend* (->DraftsetId "missing"))))))

(deftest get-draftset-owner-test
  (testing "With owner"
    (let [draftset-id "test"]
      (gen/generate-in *test-backend* {:draftsets [{:id draftset-id :created-by test-editor :owned-by test-editor}]})
      (let [owner (get-draftset-owner *test-backend* (->DraftsetId draftset-id))]
        (is (= (user/username test-editor) owner)))))

  (testing "With no owner"
    (let [draftset-id (str (UUID/randomUUID))
          spec {:draftsets [{:id draftset-id :created-by test-editor :submission ::gen/gen}]}]
      (gen/generate-in *test-backend* spec)
      (let [owner (get-draftset-owner *test-backend* (->DraftsetId draftset-id))]
        (is (nil? owner))))))

(deftest is-draftset-owner?-test
  (testing "Is owner"
    (let [draftset-id (str (UUID/randomUUID))
          spec {:draftsets [{:id draftset-id :created-by test-editor :owned-by test-editor}]}]
      (gen/generate-in *test-backend* spec)
      (is (= true (is-draftset-owner? *test-backend* (->DraftsetId draftset-id) test-editor)))))

  (testing "Has no owner"
    (let [draftset-id (str (UUID/randomUUID))
          spec {:draftsets [{:id draftset-id :created-by test-editor :submission ::gen/gen}]}]
      (gen/generate-in *test-backend* spec)
      (is (= false (is-draftset-owner? *test-backend* (->DraftsetId draftset-id) test-editor)))))

  (testing "Has different owner"
    (let [draftset-id (str (UUID/randomUUID))
          spec {:draftsets [{:id draftset-id :created-by test-publisher :owned-by test-editor}]}]
      (gen/generate-in *test-backend* spec)
      (is (= false (is-draftset-owner? *test-backend* (->DraftsetId draftset-id) test-publisher))))))

(defn- draftset-statements-exist? [db draftset-id]
  (let [draftset-uri (draftset-id->uri draftset-id)
        q (str "ASK WHERE {"
               (with-state-graph "{ <" draftset-uri "> ?p ?o . }"
                                 "UNION {"
                                 "  <" draftset-uri "> <" drafter:hasSubmission "> ?submission ."
                                 "  ?submission ?sp ?so ."
                                 "}")
               "}")]
    (repo/query db q)))

(deftest delete-draftset-statements!-test
  (testing "With owner"
    (let [draftset-id (create-draftset! *test-backend* test-editor "Test draftset")
          spec {:draftsets [{:id draftset-id :created-by test-editor :title "Test draftset" :owned-by test-editor}]}]
      (gen/generate-in *test-backend* spec)
      (delete-draftset-statements! *test-backend* (->DraftsetId draftset-id))
      (is (= false (draftset-statements-exist? *test-backend* draftset-id)))))

  (testing "With submission"
    (let [draftset-id (str (UUID/randomUUID))
          spec {:draftsets [{:id draftset-id :created-by test-editor :title "Test draftset" :description "DESCRIPTION" :submission ::gen/gen}]}]
      (gen/generate-in *test-backend* spec)
      (delete-draftset-statements! *test-backend* (->DraftsetId draftset-id))
      (is (= false (draftset-statements-exist? *test-backend* draftset-id))))))

(deftest delete-draftset!-test
  (let [draftset-id (str (UUID/randomUUID))
        ns (range 1 10)
        draftset-uris (mapv #(URI. (str "http://draft" %)) (range 1 3))
        live-graphs (map #(URI. (str "http://live" %)) ns)
        draft-graphs (map #(URI. (str "http://draft" %)) ns)
        mg-spec (into {} (map (fn [mg dg] [mg {:drafts {dg ::gen/gen}}]) live-graphs draft-graphs))
        spec {:draftsets [{:id draftset-id :owned-by test-editor}]
              :managed-graphs mg-spec}]
    (gen/generate-in *test-backend* spec)

    (doseq [dg draftset-uris]
      (is (= true (mgmth/draft-exists? *test-backend* dg))))

    (delete-draftset! *test-backend* (->DraftsetId draftset-id))

    (is (= false (draftset-exists? *test-backend* (->DraftsetId draftset-id))))

    (doseq [dg draftset-uris]
      (is (= false (mgmth/draft-exists? *test-backend* dg))))))

(deftest get-draftest-graph-mapping-test
  (let [ns (range 1 10)
        live-graphs (map #(URI. (str "http://live" %)) ns)
        draft-graphs (map #(URI. (str "http://draft" %)) ns)
        draftset-id (str (UUID/randomUUID))
        mg-spec (into {} (map (fn [mg dg] [mg {:drafts {dg ::gen}}]) live-graphs draft-graphs))
        spec {:managed-graphs mg-spec
              :draftsets [{:id draftset-id :created-by test-editor}]}]
    (gen/generate-in *test-backend* spec)
    (let [mapping (get-draftset-graph-mapping *test-backend* (->DraftsetId draftset-id))]
      (is (= (zipmap live-graphs draft-graphs) mapping)))))

(deftest delete-draftest-graph!-test
  (testing "Delete non-existent live graph"
    (let [draftset-id (create-draftset! *test-backend* test-editor "Test draftset")
          graph-to-delete (URI. "http://missing")]
      (delete-draftset-graph! *test-backend* draftset-id graph-to-delete)

      (is (= false (mgmt/is-graph-managed? *test-backend* graph-to-delete)))
      (is (empty? (get-draftset-graph-mapping *test-backend* draftset-id)))))

  (testing "Delete live graph not already in draftset"
    (let [live-graph (URI. "http://live")
          draftset-id (create-draftset! *test-backend* test-editor "Test draftset")]
      (make-graph-live! *test-backend* live-graph)
      (delete-draftset-graph! *test-backend* draftset-id live-graph)

      (is (mgmt/is-graph-managed? *test-backend* live-graph))

      (let [graph-mapping (get-draftset-graph-mapping *test-backend* draftset-id)]
        (is (contains? graph-mapping live-graph))

        (is (mgmth/draft-exists? *test-backend* (get graph-mapping live-graph))))))

  (testing "Graph already in draftset"
    (let [live-graph (URI. "http://live")
          draftset-id (create-draftset! *test-backend* test-editor "Test draftset")]
      (make-graph-live! *test-backend* live-graph)

      (let [draft-graph (import-data-to-draft! *test-backend* live-graph (test-triples (URI. "http://subject")) draftset-id)]
        (is (mgmth/draft-exists? *test-backend* draft-graph))

        (delete-draftset-graph! *test-backend* draftset-id live-graph)

        (is (mgmth/draft-exists? *test-backend* draft-graph))
        (is (= false (ask? (format "GRAPH <%s> { ?s ?p ?o }" draft-graph))))))))

(defn- draftset-has-claim-role? [draftset-id role]
  (let [q (str
           "ASK WHERE {"
           (with-state-graph
             "<" (->draftset-uri draftset-id) "> <" drafter:hasSubmission "> ?submission ."
             "?submission <" drafter:claimRole "> \"" (name role) "\" .")
           "}")]
    (query *test-backend* q)))

(deftest submit-draftset-to-role-test!
  (testing "Existing owner"
    (let [draftset-id (create-draftset! *test-backend* test-editor "Test draftset")
          draftset-uri (->draftset-uri draftset-id)]
      (submit-draftset-to-role! *test-backend* draftset-id test-editor :publisher)

      (is (draftset-has-claim-role? draftset-id :publisher))
      (is (= false (has-any-object? draftset-uri drafter:hasOwner)))))

  (testing "Submitted by other user"
    (let [draftset-id (create-draftset! *test-backend* test-editor "Test draftset")
          draftset-uri (->draftset-uri draftset-id)]
      (submit-draftset-to-role! *test-backend* draftset-id test-publisher :manager)

      (is (is-draftset-owner? *test-backend* draftset-id test-editor))
      (is (= false (has-any-object? draftset-uri drafter:hasSubmission))))))

(defn- draftset-has-claim-user? [draftset-id user]
  (let [q (str
           "ASK WHERE {"
           (with-state-graph
             "<" (->draftset-uri draftset-id) "> <" drafter:hasSubmission "> ?submission ."
             "?submission <" drafter:claimUser "> <" (user/user->uri user) "> .")
           "}")]
    (query *test-backend* q)))

(defn is-draftset-submitter? [backend draftset-ref user]
  (if-let [{:keys [submitted-by]} (get-draftset-info backend draftset-ref)]
    (= submitted-by (user/username user))
    false))

(deftest submit-draftset-to-user!-test
  (testing "When owned"
    (let [draftset-id (create-draftset! *test-backend* test-editor "Test draftset")]
      (submit-draftset-to-user! *test-backend* draftset-id test-editor test-publisher)

      (is (= nil (get-draftset-owner *test-backend* draftset-id)))
      (is (is-draftset-submitter? *test-backend* draftset-id test-editor))
      (is (draftset-has-claim-user? draftset-id test-publisher))))

  (testing "Submitted to self"
    (let [draftset-id (create-draftset! *test-backend* test-editor)
          draftset-uri (str (->draftset-uri draftset-id))]
      (submit-draftset-to-user! *test-backend* draftset-id test-editor test-editor)

      (is-draftset-owner? *test-backend* draftset-id test-editor)))

  (testing "When not owned"
    (let [draftset-id (create-draftset! *test-backend* test-editor)
          draftset-uri (str (->draftset-uri draftset-id))]

      (submit-draftset-to-user! *test-backend* draftset-id test-manager test-publisher)

      (is (is-draftset-owner? *test-backend* draftset-id test-editor))
      (is (= false (draftset-has-claim-user? draftset-id test-publisher)))
      (is (= false (is-draftset-submitter? *test-backend* draftset-id test-manager)))
      (is (= false (has-uri-object? draftset-uri drafter:submittedBy (user/user->uri test-manager))))))

  (testing "When submitted"
    (let [draftset-id (create-draftset! *test-backend* test-editor)]
      (submit-draftset-to-role! *test-backend* draftset-id test-editor :publisher)
      (submit-draftset-to-user! *test-backend* draftset-id test-editor test-manager)

      (is (= nil (get-draftset-owner *test-backend* draftset-id)))
      (is (draftset-has-claim-role? draftset-id :publisher))
      (is (= false (draftset-has-claim-user? draftset-id test-manager))))))

(defn- draftset-has-submission? [draftset-ref]
  (let [draftset-uri (->draftset-uri draftset-ref)
        q (str "ASK WHERE {"
               (with-state-graph
                 "<" draftset-uri "> <" rdf:a "> <" drafter:DraftSet "> ."
                 "<" draftset-uri "> <" drafter:hasSubmission "> ?submission")
               "}")]
    (query *test-backend* q)))

(deftest claim-draftset-test!
  (testing "No owner when user in role"
    (let [draftset-id (create-draftset! *test-backend* test-editor "Test draftset")
          draftset-uri (->draftset-uri draftset-id)]
      (submit-draftset-to-role! *test-backend* draftset-id test-editor :publisher)

      (let [[result _] (claim-draftset! *test-backend* draftset-id test-publisher)
            ds-info (get-draftset-info *test-backend* draftset-id)]
        (is (= :ok result))
        (is (is-draftset-owner? *test-backend* draftset-id test-publisher))
        (is (= false (has-any-object? draftset-uri drafter:hasSubmission))))))

  (testing "No owner when submitted to user"
    (let [draftset-id (create-draftset! *test-backend* test-editor "Test draftset")]
      (submit-draftset-to-user! *test-backend* draftset-id test-editor test-publisher)

      (let [[result _] (claim-draftset! *test-backend* draftset-id test-publisher)]
        (is (= :ok result))
        (is (is-draftset-owner? *test-backend* draftset-id test-publisher))
        (is (= false (draftset-has-submission? draftset-id))))))

  (testing "No owner when submitted to different user"
    (let [draftset-id (create-draftset! *test-backend* test-editor "Test draftset")]
      (submit-draftset-to-user! *test-backend* draftset-id test-editor test-publisher)

      (let [[result _] (claim-draftset! *test-backend* draftset-id test-manager)]
        (is (= :user result))
        (is (nil? (get-draftset-owner *test-backend* draftset-id)))
        (is (draftset-has-submission? draftset-id)))))

  (testing "Reclaimed by submitter after submit to role"
    (let [draftset-id (create-draftset! *test-backend* test-editor "Test draftset")]
      (submit-draftset-to-role! *test-backend* draftset-id test-editor :publisher)
      (let [[result _] (claim-draftset! *test-backend* draftset-id test-editor)]
        (is (= :ok result))
        (is (is-draftset-owner? *test-backend* draftset-id test-editor))
        (is (= false (draftset-has-submission? draftset-id))))))

  (testing "Reclaimed by submitter after submit to user"
    (let [draftset-id (create-draftset! *test-backend* test-editor "Test draftset")]
      (submit-draftset-to-user! *test-backend* draftset-id test-editor test-publisher)
      (let [[result _] (claim-draftset! *test-backend* draftset-id test-editor)]
        (is (= :ok result))
        (is (is-draftset-owner? *test-backend* draftset-id test-editor))
        (is (= false (draftset-has-submission? draftset-id))))))

  (testing "No owner when user is submitter not in role"
    (let [draftset-id (create-draftset! *test-backend* test-editor "Test draftset")]
      (submit-draftset-to-role! *test-backend* draftset-id test-editor :publisher)
      (let [[result _] (claim-draftset! *test-backend* draftset-id test-editor)]
        (is (= :ok result))
        (is (is-draftset-owner? *test-backend* draftset-id test-editor))
        (is (= false (has-any-object? (->draftset-uri draftset-id) drafter:hasSubmission))))))

  (testing "Owned by other user after submitted by user"
    (let [draftset-id (create-draftset! *test-backend* test-editor "Test draftset")]
      (submit-draftset-to-role! *test-backend* draftset-id test-editor :publisher)
      (claim-draftset! *test-backend* draftset-id test-publisher)
      (let [[result _] (claim-draftset! *test-backend* draftset-id test-editor)]
        (is (= :owned result)))))

  (testing "Claimed by current owner"
    (let [draftset-id (create-draftset! *test-backend* test-editor)
          [result _] (claim-draftset! *test-backend* draftset-id test-editor)]
      (is (= :ok result))
      (is (is-draftset-owner? *test-backend* draftset-id test-editor))))

  (testing "User not in claim role"
    (let [draftset-id (create-draftset! *test-backend* test-editor)]
      (submit-draftset-to-role! *test-backend* draftset-id test-editor :manager)
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

(deftest revert-changes-from-graph-only-in-draftset
  (let [live-graph (URI. "http://live")
        draftset-id (create-draftset! *test-backend* test-editor)]
    (mgmt/create-managed-graph! *test-backend* live-graph)
    (let [draft-graph (mgmt/create-draft-graph! *test-backend* live-graph draftset-id)
          result (revert-graph-changes! *test-backend* draftset-id live-graph)]
      (is (= :reverted result))
      (is (= false (mgmth/draft-exists? *test-backend* draft-graph)))
      (is (= false (mgmt/is-graph-managed? *test-backend* live-graph))))))

(deftest revert-changes-from-graph-which-exists-in-live
  (let [live-graph-uri (make-graph-live! *test-backend* (URI. "http://live"))
        draftset-id (create-draftset! *test-backend* test-editor)
        draft-graph-uri (delete-draftset-graph! *test-backend* draftset-id live-graph-uri)]
    (let [result (revert-graph-changes! *test-backend* draftset-id live-graph-uri)]
      (is (= :reverted result))
      (is (mgmt/is-graph-managed? *test-backend* live-graph-uri))
      (is (= false (mgmth/draft-exists? *test-backend* draft-graph-uri))))))

(deftest revert-change-from-graph-which-exists-independently-in-other-draftset
  (let [live-graph-uri (mgmt/create-managed-graph! *test-backend* (URI. "http://live"))
        ds1-id (create-draftset! *test-backend* test-editor)
        ds2-id (create-draftset! *test-backend* test-publisher)
        draft-graph1-uri (mgmt/create-draft-graph! *test-backend* live-graph-uri ds1-id)
        draft-graph2-uri (mgmt/create-draft-graph! *test-backend* live-graph-uri ds2-id)]

    (let [result (revert-graph-changes! *test-backend* ds2-id live-graph-uri)]
      (is (= :reverted result))
      (is (mgmt/is-graph-managed? *test-backend* live-graph-uri))
      (is (= false (mgmth/draft-exists? *test-backend* draft-graph2-uri)))
      (is (mgmth/draft-exists? *test-backend* draft-graph1-uri)))))

(deftest revert-non-existent-change-in-draftset
  (let [draftset-id (create-draftset! *test-backend* test-editor)
        result (revert-graph-changes! *test-backend* draftset-id (URI. "http://missing"))]
    (is (= :not-found result))))

(deftest revert-changes-in-non-existent-draftset
  (let [live-graph (make-graph-live! *test-backend* (URI. "http://live"))
        result (revert-graph-changes! *test-backend* (->DraftsetId "missing") live-graph)]
    (is (= :not-found result))))

(defn- get-graph-triples [graph-uri]
  (let [results (query *test-backend* (select-all-in-graph graph-uri))]
    (map (fn [{:keys [s p o]}] (->Triple s p o)) results)))

(deftest copy-live-graph-into-draftset-test
  (let [draftset-id (create-draftset! *test-backend* test-editor)
        live-triples (test-triples (URI. "http://test-subject"))
        live-graph-uri (make-graph-live! *test-backend* (URI. "http://live") live-triples)
        {:keys [value-p] :as copy-job} (copy-live-graph-into-draftset-job *test-backend* draftset-id live-graph-uri)]
    (scheduler/queue-job! copy-job)

    @value-p

    (let [draft-graph (find-draftset-draft-graph *test-backend* draftset-id live-graph-uri)
          draft-triples (get-graph-triples draft-graph)]
      (is (= (set live-triples) (set draft-triples))))))

(deftest copy-live-graph-into-existing-draft-graph-in-draftset-test
  (let [draftset-id (create-draftset! *test-backend* test-editor)
        live-triples (test-triples (URI. "http://test-subject"))
        live-graph-uri (make-graph-live! *test-backend* (URI. "http://live") live-triples)
        initial-draft-triples (test-triples (URI. "http://temp-subject"))
        draft-graph-uri (import-data-to-draft! *test-backend* live-graph-uri initial-draft-triples draftset-id)
        {:keys [value-p] :as copy-job} (copy-live-graph-into-draftset-job *test-backend* draftset-id live-graph-uri)]

    (scheduler/queue-job! copy-job)
    @value-p

    (let [draft-triples (get-graph-triples draft-graph-uri)]
      (is (= (set live-triples) (set draft-triples))))))

(deftest quad-batch->graph-triples-test
  (testing "Empty batch"
    (is (thrown? IllegalArgumentException (quad-batch->graph-triples []))))

  (testing "Non-empty batch"
    (let [guri "http://graph"
          quads (map #(->Quad (str "http://s" %) (str "http://p" %) (str "http://o" %) guri) (range 1 10))
          {:keys [graph-uri triples]} (quad-batch->graph-triples quads)]
      (is (= guri graph-uri))
      (is (every? identity (map triple= quads triples))))))

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
                  (query modified-query))]

      (assert (= 1 (count res)) "There were multiple modifiedAt timestamps, we expect just one.")

      (-> res first :modified))))

(deftest append-data-to-draftset-job-test
  (let [ds (create-draftset! *test-backend* test-editor)]
    (apply-job! (append-triples-to-draftset-job *test-backend* ds (io/file "./test/test-triple.nt") RDFFormat/NTRIPLES (URI. "http://foo/graph")))
    (let [ts-1 (fetch-draft-graph-modified-at ds "http://foo/graph")]
      (apply-job! (append-triples-to-draftset-job *test-backend* ds (io/file "./test/test-triple-2.nt") RDFFormat/NTRIPLES (URI. "http://foo/graph")))
      (let [ts-2 (fetch-draft-graph-modified-at ds "http://foo/graph")]
        (is (< (.getTime ts-1)
               (.getTime ts-2))
            "Modified time is updated after append")

        (apply-job! (delete-triples-from-draftset-job *test-backend* ds (URI. "http://foo/graph") (io/file "./test/test-triple-2.nt") RDFFormat/NTRIPLES))
        (let [ts-3 (fetch-draft-graph-modified-at ds "http://foo/graph")]
          (is (< (.getTime ts-2)
               (.getTime ts-3))
              "Modified time is updated after delete"))))))

(use-fixtures :once wrap-db-setup)
(use-fixtures :each wrap-clean-test-db)
