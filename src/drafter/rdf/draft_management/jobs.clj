(ns drafter.rdf.draft-management.jobs
  (:require [clojure.tools.logging :as log]
            [swirrl-server.responses :as restapi]
            [drafter.backend.protocols :refer :all]
            [drafter.common.api-routes :refer [meta-params]]
            [drafter.rdf.draft-management :as mgmt]
            [swirrl-server.async.jobs :refer [create-job complete-job! create-child-job]]
            [drafter.write-scheduler :refer [queue-job!]]
            [drafter.rdf.drafter-ontology :refer :all]
            [drafter.util :as util]
            [grafter.vocabularies.rdf :refer :all]
            [grafter.rdf :refer [statements]]
            [grafter.rdf.protocols :refer [update!]]
            [grafter.rdf.io :refer [mimetype->rdf-format]]
            [grafter.rdf.repository :refer [query with-transaction ToConnection ->connection]]
            [environ.core :refer [env]]
            [clojure.string :as string]))

;; Note if we change this default value we should also change it in the
;; drafter-client, and possibly other places too.
(def batched-write-size (Integer/parseInt (get env :drafter-batched-write-size "10000")))

(defmacro with-job-exception-handling [job & forms]
  `(try
     ~@forms
     (catch clojure.lang.ExceptionInfo exi#
       (complete-job! ~job {:type :error
                            :error-type (str (class exi#))
                            :exception (.getCause exi#)}))
     (catch Exception ex#
       (complete-job! ~job {:type :error
                            :error-type (str (class ex#))
                            :exception ex#}))))

(defn failed-job-result?
  "Indicates whether the given result object is a failed job result."
  [{:keys [type] :as result}]
  (= :error type))

(defmacro make-job [write-priority [job :as args] & forms]
  `(create-job ~write-priority
               (fn [~job]
                 (with-job-exception-handling ~job
                   ~@forms))))

(defn job-succeeded!
  "Adds the job to the set of finished-jobs as a successfully completed job."
  ([job] (job-succeeded! job {}))
  ([job details] (complete-job! job (merge {:type :ok} details))))

(defn create-draft-job [repo live-graph params]
  (make-job :sync-write [job]
            (let [conn (->connection repo)
                  draft-graph-uri (with-transaction conn
                                    (mgmt/create-managed-graph! conn live-graph)
                                    (mgmt/create-draft-graph! conn
                                                              live-graph (meta-params params)))]

              (job-succeeded! job {:guri draft-graph-uri}))))

(defn- finish-delete-job! [repo graph contents-only? job]
  (when-not contents-only?
    (mgmt/delete-draft-graph-state! repo graph))
  (job-succeeded! job))

(defn delete-in-batches [repo graph contents-only? job]
  ;; Loops until the graph is empty, then deletes state graph if not a
  ;; contents-only? deletion.
  ;;
  ;; Checks that graph is a draft graph - will only delete drafts.
  (if (and (mgmt/graph-exists? repo graph)
           (mgmt/draft-exists? repo graph))
      (do
        (with-open [conn (->connection repo)]
          (with-transaction conn
            (mgmt/delete-graph-batched! conn graph batched-write-size)))
        

        (if (mgmt/graph-exists? repo graph)
          ;; There's more graph contents so queue another job to continue the
          ;; deletions.
          (let [apply-next-batch (partial delete-in-batches repo graph contents-only?)]
            (queue-job! (create-child-job job apply-next-batch)))
          (finish-delete-job! repo graph contents-only? job)))
      (finish-delete-job! repo graph contents-only? job)))

(defn copy-graph-batch-query
  "Query to copy a range of data in a source graph into a destination
  graph."
  [source-graph dest-graph offset limit]
  (str "INSERT {
          GRAPH <" dest-graph "> {
            ?s ?p ?o
          }
        } WHERE {
          SELECT ?s ?p ?o WHERE {
            GRAPH <" source-graph "> {
              ?s ?p ?o
            }
          } LIMIT " limit " OFFSET " offset "
       }"))

(defn copy-graph-batch!
  "Copies the data segmented by offset and limit in the source graph
  into the given destination graph."
  [repo source-graph dest-graph offset limit]
  (let [query (copy-graph-batch-query source-graph dest-graph offset limit)]
    (update! repo query)))

(defn calculate-offsets [count batch-size]
  "Given a total number of items and a batch size, returns a sequence
  of [offset limit] pairs for segmenting the source collection into
  batches.

  The limit will always be set to batch-size."
  (take (/ count batch-size)
        (iterate (fn [[offset batch-size]]
                   [(+ offset batch-size) batch-size])
                 [0 batch-size])))

(defn graph-count-query [graph]
  (str "SELECT (COUNT(*) as ?c) WHERE {
          GRAPH <" graph "> { ?s ?p ?o }
        }"))

(defn get-graph-clone-batches
  ([repo graph-uri] (get-graph-clone-batches repo graph-uri batched-write-size))
  ([repo graph-uri batch-size]
   (let [m (first (query repo (graph-count-query graph-uri)))
         graph-count (Integer/parseInt (.stringValue (get m "c")))]
     (calculate-offsets graph-count batch-size))))

;;update-graph-metadata :: Repository -> [URI] -> Seq [String, String] -> Job -> ()
(defn- update-graph-metadata
  "Updates or creates each of the the given graph metadata pairs for
  each given graph under a job."
  [repo graphs metadata job]
  (with-job-exception-handling job
    (with-open [conn (->connection repo)]
      (doseq [draft-graph graphs]
        (mgmt/add-metadata-to-graph conn draft-graph metadata))
      (complete-job! job restapi/ok-response))))

(defn create-update-metadata-job
  "Creates a job to associate the given graph metadata pairs with each
  given graph."
  [repo graphs metadata]
  (create-job :sync-write (partial update-graph-metadata repo graphs metadata)))

(defn- sparql-uri-list [uris]
  (string/join " " (map #(str "<" % ">") uris)))

(defn- delete-graph-metadata-query [graph-uris meta-uris]
  (str
   "DELETE {"
   (mgmt/with-state-graph "?g ?meta ?o")
   "} WHERE {"
   (mgmt/with-state-graph
     (str
      "?g ?meta ?o
       VALUES ?g {" (sparql-uri-list graph-uris) "}
       VALUES ?meta {" (sparql-uri-list meta-uris) "}"))
   "}"))

(defn- delete-graph-metadata
  "Deletes all metadata values associated with the given keys across
  all the given graph URIs."
  [repo graphs meta-keys job]
  (with-job-exception-handling job
    (let [meta-uris (map meta-uri meta-keys)
          delete-query (delete-graph-metadata-query graphs meta-uris)]
      (update! repo delete-query)
      (complete-job! job restapi/ok-response))))

(defn create-delete-metadata-job
  "Create a job to delete the given metadata keys from a collection of
  draft graphs."
  [repo graphs meta-keys]
  (create-job :sync-write (partial delete-graph-metadata repo graphs meta-keys)))


(defn- copy-from-live-graph [repo live-graph-uri dest-graph-uri batches job]
  (with-job-exception-handling job
    (if-let [[offset limit] (first batches)]
      (let [next-fn (fn [job]
                      (copy-from-live-graph repo live-graph-uri dest-graph-uri (rest batches) job))]
        (copy-graph-batch! repo live-graph-uri dest-graph-uri offset limit)
        (queue-job! (create-child-job job next-fn)))
      (job-succeeded! job))))

(defn create-copy-from-live-graph-job [repo draft-graph-uri]
    (make-job :batch-write [job]
              (let [live-graph-uri (mgmt/lookup-live-graph repo draft-graph-uri)
                    batch-sizes (and live-graph-uri
                                     (get-graph-clone-batches repo live-graph-uri))]
                (copy-from-live-graph repo live-graph-uri draft-graph-uri batch-sizes job))))

(defn migrate-graph-live-job [repo graphs]
  (make-job :exclusive-write [job]
            (log/info "Starting make-live for graph" graphs)
            (let [conn (->connection repo)]
              (with-transaction conn
                (doseq [g graphs]
                  (mgmt/migrate-live! conn g))))
            (log/info "Make-live for graphs " graphs " done")
            (job-succeeded! job)))
