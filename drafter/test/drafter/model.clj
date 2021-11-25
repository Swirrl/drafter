(ns drafter.model
  "Defines an interface for all test drafter operations and contains an implementation which invokes handler
   operations directly instead of going via the ring API. Also contains a make-sync function which converts all
   asynchronous operations into ones which block waiting for inner operations to complete."
  (:require [clojure.test :refer :all]
            [drafter.backend.draftset.operations :as dsops]
            [drafter.feature.draftset.list :as ds-list]
            [drafter.feature.draftset.changes :as ds-changes]
            [drafter.feature.draftset.show :as ds-show]
            [drafter.feature.draftset-data.append :as ds-append]
            [drafter.feature.draftset-data.append-by-graph :as ds-append-by-graph]
            [drafter.feature.draftset-data.delete-by-graph :as ds-delete-by-graph]
            [drafter.feature.draftset-data.delete :as ds-delete]
            [drafter.user :as user]
            [drafter.util :as util]
            [drafter.backend :as backend]
            [grafter-2.rdf.protocols :as pr]
            [drafter.backend.draftset.graphs :as graphs]
            [drafter.backend.draftset :as draftsets]
            [grafter-2.rdf4j.repository :as repo]
            [clojure.test :as t]
            [drafter.test-common :as tc]
            [drafter.user-test :refer [test-publisher test-editor]]
            [drafter.rdf.sesame :as ses]
            [drafter.responses :as responses]
            [drafter.rdf.draftset-management.jobs :as dsjobs]
            [martian.encoders :as enc]
            [drafter.write-scheduler :as writes]
            [drafter.feature.draftset.update :as update]
            [drafter.write-scheduler :as write-scheduler]
            [drafter.time :as time]
            [drafter.feature.draftset.update :as update]
            [drafter.feature.draftset.test-helper :as help]
            [clojure.set :as set])
  (:import [java.time OffsetDateTime ZoneOffset]
           [java.net URI]
           [clojure.lang ExceptionInfo]
           [java.lang AutoCloseable]))

(defn format-metadata [metadata]
  (some-> metadata enc/json-encode))

(defn- create-draftset-op [{:keys [backend clock] :as drafter} user {:keys [display-name description id] :as opts}]
  (let [id-fn (if (some? id) (constantly id) util/create-uuid)]
    (dsops/create-draftset! backend user display-name description id-fn clock)))

(defn- revert-draftset-graph-changes-op [{:keys [backend] :as drafter} _user draftset-ref graph-uri]
  (let [result (ds-changes/revert-graph-changes! drafter draftset-ref graph-uri)]
    (case result
      :reverted (dsops/get-draftset-info backend draftset-ref)
      :not-found (throw (ex-info (format "Graph %s not found" graph-uri)
                                 {:graph graph-uri :draftset draftset-ref}))
      (throw (ex-info "Unexpected revert result" {:result result})))))

(defn- claim-draftset-op [{:keys [backend] :as drafter} user draftset-ref]
  (if-let [ds-info (dsops/get-draftset-info backend draftset-ref)]
    (if (user/can-claim? user ds-info)
      (let [[outcome ds-info] (dsops/claim-draftset! backend draftset-ref user)]
        (if (= :ok outcome)
          ds-info
          (throw (ex-info "Failed to claim draftset" {}))))
      (throw (ex-info "User cannot claim draftset" {})))
    (throw (ex-info "Draftset not found" {}))))

(defn- delete-draftset-op [{:keys [backend] :as drafter} user draftset-ref {:keys [metadata] :as opts}]
  (let [params (cond-> {:draftset-id draftset-ref}
                       (some? metadata) (assoc :metadata (format-metadata metadata)))
        job (dsjobs/delete-draftset-job backend
                                        user
                                        params)]
    (responses/enqueue-async-job! job)))

(defn- list-draftsets-op [{:keys [backend] :as drafter} user {:keys [include union-with-live?] :as opts}]
  (ds-list/get-draftsets backend user (or include :all) (or union-with-live? false)))

(defn- draftset-options-op [{:keys [backend] :as drafter} user draftset-ref]
  (dsops/find-permitted-draftset-operations backend draftset-ref user))

(defn- publish-draftset-op [drafter user draftset-ref {:keys [metadata] :as opts}]
  (if (user/has-role? user :publisher)
    (let [params (cond-> {:draftset-id draftset-ref}
                         (some? metadata) (assoc :metadata (format-metadata metadata)))
          job (dsjobs/publish-draftset-job drafter
                                           user
                                           params)]
      (responses/enqueue-async-job! job))
    (throw (ex-info "You require the publisher role to perform this action" {}))))

(defn- draftset-query-endpoint-op [{:keys [backend]} _user draftset-ref {:keys [union-with-live?] :as opts}]
  (backend/endpoint-repo backend draftset-ref {:union-with-live? union-with-live?}))

(defn- set-draftset-metadata-op [{:keys [backend] :as drafter} _user draftset-ref metadata]
  (dsops/set-draftset-metadata! backend draftset-ref metadata)
  (dsops/get-draftset-info backend draftset-ref))

(defn- show-draftset-op [{:keys [backend] :as drafter} user draftset-ref {:keys [union-with-live?] :as opts}]
  (ds-show/get-draftset backend user draftset-ref (or union-with-live? false)))

(defn- submit-draftset-op [{:keys [backend]} user draftset-ref {target-user :user target-role :role :as opts}]
  (cond
    (and (some? target-user) (some? target-role)) (throw (ex-info "Specify only one of target user or role" {}))
    (some? target-user) (dsops/submit-draftset-to-user! backend draftset-ref user target-user)
    (some? target-role) (dsops/submit-draftset-to-role! backend draftset-ref user target-role)
    :else (throw (ex-info "Target user or role required" {})))
  (dsops/get-draftset-info backend draftset-ref))

(defn- append-draftset-data-op [drafter user draftset-ref source {:keys [metadata] :as opts}]
  (ds-append/append-data drafter user draftset-ref source (format-metadata metadata)))

(defn- copy-live-graph-into-draftset-op [drafter user draftset-ref graph {:keys [metadata] :as opts}]
  (ds-append-by-graph/copy-live-graph drafter user draftset-ref graph metadata))

(defn- delete-draftset-data-op [drafter user draftset-ref source {:keys [metadata] :as opts}]
  (ds-delete/delete-data drafter user draftset-ref source metadata))

(defn- delete-draftset-graph-op [drafter user draftset-ref graph {:keys [silent] :as opts}]
  (ds-delete-by-graph/delete-graph drafter user draftset-ref graph (or silent false)))

(defn- get-draftset-data-op [{:keys [backend] :as drafter} _user draftset-ref {:keys [graph union-with-live?]}]
  (let [union-with-live? (or union-with-live? false)
        executor (draftsets/build-draftset-endpoint backend draftset-ref union-with-live?)
        is-triples-query? (some? graph)
        pquery (if is-triples-query?
                 (dsops/all-graph-triples-query executor graph)
                 (dsops/all-quads-query executor))]
    (repo/evaluate pquery)))

(defn- live-query-endpoint-op [{:keys [backend] :as drafter}]
  (backend/endpoint-repo backend ::backend/live))

(defn- get-setting [drafter setting]
  (get-in drafter [:settings setting]))

(defn- submit-update-request-op [drafter user draftset-ref update-query]
  (let [max-update-size (get-setting drafter :max-update-size)
        update (update/parse-update-query drafter update-query max-update-size)]
    (update/update! drafter max-update-size draftset-ref update)))

(defrecord ModelDrafter [backend clock graph-manager global-writes-lock writes-handle operations settings]
  AutoCloseable
  (close [_this]
    (writes/stop-writer! writes-handle)))

(defn- calculate-settings [settings]
  (let [default-settings {:max-update-size 5000}]
    (merge default-settings settings)))

(defn create
  "Creates a new model with the given backend database and time source"
  ([repo] (create repo {}))
  ([repo {:keys [clock graph-manager settings]}]
   (let [clock (or clock time/system-clock)
         graph-manager (or graph-manager (graphs/create-manager repo #{} clock))
         global-writes-lock (write-scheduler/create-writes-lock)
         operations {:revert-draftset-graph-changes revert-draftset-graph-changes-op
                     :claim-draftset                claim-draftset-op
                     :create-draftset               create-draftset-op
                     :delete-draftset               delete-draftset-op
                     :list-draftsets                list-draftsets-op
                     :draftset-options              draftset-options-op
                     :publish-draftset              publish-draftset-op
                     :draftset-query-endpoint       draftset-query-endpoint-op
                     :set-draftset-metadata         set-draftset-metadata-op
                     :show-draftset                 show-draftset-op
                     :submit-draftset               submit-draftset-op

                     :live-query-endpoint           live-query-endpoint-op

                     :append-draftset-data          append-draftset-data-op
                     :copy-live-graph-into-draftset copy-live-graph-into-draftset-op
                     :delete-draftset-data          delete-draftset-data-op
                     :delete-draftset-graph         delete-draftset-graph-op
                     :get-draftset-data             get-draftset-data-op

                     :submit-update-request         submit-update-request-op}
         writes-handle (writes/start-writer! global-writes-lock)
         settings (calculate-settings settings)]
     (->ModelDrafter repo clock graph-manager global-writes-lock writes-handle operations settings))))

(defn make-sync
  "Returns a model implementation which converts all asynchronous operations (i.e. those which return
   job responses) into synchronous operations which wait up to a configurable timeout for the inner
   job to complete."
  ([drafter] (make-sync drafter 10))
  ([drafter timeout-seconds]
   (letfn [(make-sync [op]
             (fn [& args]
               (let [job (apply op args)
                     timeout-ms (* 1000 timeout-seconds)
                     result (deref job timeout-ms ::timeout)]
                 (cond
                   (= ::timeout result)
                   (throw (ex-info "Timed out waiting for job" {}))

                   (= :ok (:type result))
                   result

                   :else
                   (throw (ex-info "Job failed" {:result result}))))))]
     (let [async-ops #{:delete-draftset :publish-draftset :append-draftset-data
                       :copy-live-graph-into-draftset :delete-draftset-data}]
       (update drafter :operations (fn [operations]
                                     (reduce (fn [acc async-op]
                                               (update acc async-op make-sync))
                                             operations
                                             async-ops)))))))

(defn- get-op [drafter op-key]
  (get-in drafter [:operations op-key]))

(defn revert-graph-changes
  "Reverts changes to a graph within a draftset"
  [drafter user draftset-ref graph]
  ((get-op drafter :revert-draftset-graph-changes) drafter user draftset-ref graph))

(defn claim-draftset
  "Claims an available draftset for a user"
  [drafter user draftset-ref]
  ((get-op drafter :claim-draftset) drafter user draftset-ref))

(defn create-draftset
  "Creates a new draftset owned by the given user"
  ([drafter user] (create-draftset drafter user {}))
  ([drafter user opts]
   ((get-op drafter :create-draftset) drafter user opts)))

(defn delete-draftset
  "Deletes a draftset owned by a user"
  ([drafter user draftset-ref] (delete-draftset drafter user draftset-ref {}))
  ([drafter user draftset-ref opts]
   ((get-op drafter :delete-draftset) drafter user draftset-ref opts)))

(defn list-draftsets
  "Lists the draftsets visible to a user"
  ([drafter user] (list-draftsets drafter user {}))
  ([drafter user opts]
   ((get-op drafter :list-draftsets) drafter user opts)))

(defn draftset-options
  "Returns the operations available to a user on a draftset"
  [drafter user draftset-ref]
  ((get-op drafter :draftset-options) drafter user draftset-ref))

(defn publish-draftset
  "Publishes a draftset owned by user"
  ([drafter user draftset-ref] (publish-draftset drafter user draftset-ref {}))
  ([drafter user draftset-ref opts]
   ((get-op drafter :publish-draftset) drafter user draftset-ref opts)))

(defn get-draftset-query-endpoint
  "Returns a repository for querying a draftset"
  ([drafter user draftset-ref] (get-draftset-query-endpoint drafter user draftset-ref {}))
  ([drafter user draftset-ref opts]
   ((get-op drafter :draftset-query-endpoint) drafter user draftset-ref opts)))

(defn get-live-query-endpoint
  "Returns a repository for querying the live data"
  [drafter]
  ((get-op drafter :live-query-endpoint) drafter))

(defn get-raw-repo
  "Returns the inner unrestricted repository for the entire database"
  [drafter]
  (:backend drafter))

(defn set-draftset-metadata [drafter user draftset-ref metadata]
  ((get-op drafter :set-draftset-metadata) drafter user draftset-ref metadata))

(defn show-draftset
  "Returns the current state of a draftset owned by user"
  ([drafter user draftset-ref] (show-draftset drafter user draftset-ref {}))
  ([drafter user draftset-ref opts]
   ((get-op drafter :show-draftset) drafter user draftset-ref opts)))

(defn- submit-draftset [drafter user draftset-ref opts]
  ((get-op drafter :submit-draftset) drafter user draftset-ref opts))

(defn submit-draftset-to-user
  "Submits a draftset owned by user to a specific user"
  [drafter user draftset-ref target-user]
  (submit-draftset drafter user draftset-ref {:user target-user}))

(defn submit-draftset-to-role
  "Submits a draftset owned by user to a role"
  [drafter user draftset-ref target-role]
  (submit-draftset drafter user draftset-ref {:role target-role}))

(defn append-data
  "Appends data from an RDF source to a draftset"
  ([drafter user draftset-ref source]
   (append-data drafter user draftset-ref source {}))
  ([drafter user draftset-ref source opts]
   ((get-op drafter :append-draftset-data) drafter user draftset-ref source opts)))

(defn copy-live-graph
  "Copies a live graph into a draftset"
  ([drafter user draftset-ref graph] (copy-live-graph drafter user draftset-ref graph {}))
  ([drafter user draftset-ref graph opts]
   ((get-op drafter :copy-live-graph-into-draftset) drafter user draftset-ref graph opts)))

(defn delete-data
  "Deletes data from an RDF source from a draftset"
  ([drafter user draftset-ref source] (delete-data drafter user draftset-ref source {}))
  ([drafter user draftset-ref source opts]
   ((get-op drafter :delete-draftset-data) drafter user draftset-ref source opts)))

(defn delete-graph
  "Deletes a graph within a draftset"
  ([drafter user draftset-ref graph] (delete-graph drafter user draftset-ref graph {}))
  ([drafter user draftset-ref graph opts]
   ((get-op drafter :delete-draftset-graph) drafter user draftset-ref graph opts)))

(defn get-data
  "Returns RDF data within a draftset. If the :graph option is specified only the data from
   the specified graph is returned otherwise all quads will be returned."
  ([drafter user draftset-ref] (get-data drafter user draftset-ref {}))
  ([drafter user draftset-ref opts]
   ((get-op drafter :get-draftset-data) drafter user draftset-ref opts)))

(defn submit-update-request
  "Submits a SPARQL UPDATE query within a draftset"
  [drafter user draftset-ref sparql-update-query]
  ((get-op drafter :submit-update-request) drafter user draftset-ref sparql-update-query))

(t/deftest operations-test
  (tc/with-system
    [:drafter/backend :drafter.fixture-data/loader]
    [system "drafter/feature/empty-db-system.edn"]
    (let [repo (:drafter/backend system)
          t1 (OffsetDateTime/of 2020 12 30 14 52 13 0 ZoneOffset/UTC)]
      (with-open [drafter (make-sync (create repo {:clock t1}))]
        (let [ds-id (create-draftset drafter test-publisher {:id "test" :display-name "Count Draftula" :description "My amazing draftset"})
              _ds (show-draftset drafter test-publisher ds-id {})
              _dsu (show-draftset drafter test-publisher ds-id {:union-with-live? true})
              _ds-list (list-draftsets drafter test-publisher {})]

          ;;lifecycle
          (submit-draftset-to-role drafter test-publisher ds-id :editor)
          (claim-draftset drafter test-editor ds-id)
          (submit-draftset-to-user drafter test-editor ds-id test-publisher)
          (claim-draftset drafter test-publisher ds-id)
          (let [_ds (show-draftset drafter test-publisher ds-id {})])

          ;;data
          (let [quads [(pr/->Quad (URI. "http://s1") (URI. "http://p1") "o1" (URI. "http://g1"))
                       (pr/->Quad (URI. "http://s1") (URI. "http://p2") "o2" (URI. "http://g1"))
                       (pr/->Quad (URI. "http://s1") (URI. "http://p3") "o3" (URI. "http://g1"))
                       (pr/->Quad (URI. "http://s2") (URI. "http://p1") "o4" (URI. "http://g2"))]
                to-delete [(pr/->Quad (URI. "http://s1") (URI. "http://p2") "o2" (URI. "http://g1"))]]
            (append-data drafter test-publisher ds-id (ses/->CollectionStatementSource quads))
            (delete-graph drafter test-publisher ds-id (URI. "http://g2") {})
            (t/is (thrown? ExceptionInfo (delete-graph drafter test-publisher ds-id (URI. "http://missing") {:silent false})))
            (delete-data drafter test-publisher ds-id (ses/->CollectionStatementSource to-delete) {})

            (let [quads (get-data drafter test-publisher ds-id {})
                  quads (filter help/is-user-quad? quads)
                  graph-triples (get-data drafter test-publisher ds-id {:graph (URI. "http://g1")})
                  query-endpoint (get-draftset-query-endpoint drafter test-publisher ds-id {:union-with-live? false})]
              (t/is (= #{(pr/->Quad (URI. "http://s1") (URI. "http://p1") "o1" (URI. "http://g1"))
                         (pr/->Quad (URI. "http://s1") (URI. "http://p3") "o3" (URI. "http://g1"))} (set quads)))
              (t/is (= #{(pr/->Triple (URI. "http://s1") (URI. "http://p1") "o1")
                         (pr/->Triple (URI. "http://s1") (URI. "http://p3") "o3")}
                       (set graph-triples)))

              (with-open [conn (repo/->connection query-endpoint)]
                (let [q "SELECT ?o WHERE { GRAPH <http://g1> { <http://s1> <http://p3> ?o } }"
                      bindings (vec (repo/query conn q))]
                  (t/is (= [{:o "o3"}] bindings)))))

            (let [live-endpoint (get-live-query-endpoint drafter)
                  triples (with-open [conn (repo/->connection live-endpoint)]
                            (set (repo/query conn "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }")))]
              (t/is (empty? triples) "Live should be empty until publish"))

            (publish-draftset drafter test-publisher ds-id)

            (let [live-endpoint (get-live-query-endpoint drafter)
                  triples (with-open [conn (repo/->connection live-endpoint)]
                            (set (repo/query conn "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }")))]
              (t/is (set/subset? #{(pr/->Triple (URI. "http://s1") (URI. "http://p1") "o1")
                                   (pr/->Triple (URI. "http://s1") (URI. "http://p3") "o3")} triples) "Live should contain published draft data"))

            (let [ds2-id (create-draftset drafter test-editor {:id "second" :display-name "Second test draftset"})
                  query-endpoint (get-draftset-query-endpoint drafter test-editor ds2-id {:union-with-live? true})
                  bindings (with-open [conn (repo/->connection query-endpoint)]
                             (vec (repo/query conn "SELECT ?p WHERE { GRAPH <http://g1> { <http://s1> ?p \"o1\" } }")))
                  to-append [(pr/->Quad (URI. "http://s3") (URI. "http://p4") "o5" (URI. "http://g1"))]]
              (t/is (= [{:p (URI. "http://p1")}] bindings))

              (append-data drafter test-editor ds2-id (ses/->CollectionStatementSource to-append))
              (revert-graph-changes drafter test-editor ds2-id (URI. "http://g1"))
              (delete-draftset drafter test-editor ds2-id))))))))