(ns drafter.rewrite-fixup-test
  (:require [cheshire.core :as json]
            [clojure.test :as t :refer [are is testing]]
            [drafter.feature.draftset.test-helper :as help]
            [drafter.test-common :as tc]
            [drafter.user-test :refer [test-publisher]]
            [grafter-2.rdf.protocols :refer [->Quad map->Quad]]
            [grafter-2.rdf4j.repository :as repo]
            [drafter.feature.draftset.create-test :as ct]
            [clojure.string :as string]
            [drafter.backend.draftset.operations :as ops]
            [clojure.walk :as walk])
  (:import java.net.URI
           java.util.UUID))

(t/use-fixtures :each tc/with-spec-instrumentation)

(def s1 (URI. "http://s1"))
(def p1 (URI. "http://p1"))
(def o1 (URI. "http://o1"))
(def s2 (URI. "http://s2"))
(def p2 (URI. "http://p2"))
(def o2 (URI. "http://o2"))
(def s3 (URI. "http://s3"))
(def p3 (URI. "http://p3"))
(def o3 (URI. "http://o3"))
(def s4 (URI. "http://s4"))
(def p4 (URI. "http://p4"))
(def o4 (URI. "http://o4"))

(defn random-uri [& [domain]]
  (URI. (str "http://" (when domain (str domain "/")) (UUID/randomUUID))))

(def system-config "test-system.edn")

(def keys-for-test
  [[:drafter/routes :draftset/api]
   :drafter/write-scheduler
   :drafter.routes.sparql/live-sparql-query-route
   :drafter.backend.live/endpoint
   :drafter.backend.draftset.graphs/manager
   :drafter.common.config/sparql-query-endpoint])

(def dg-q
  "SELECT ?o WHERE { GRAPH ?g { <%s> <http://publishmydata.com/def/drafter/hasDraft> ?o } }")

(defn draft-graph-uri-for [conn graph-uri]
  (:o (first (repo/query conn (format dg-q graph-uri)))))

(def ds-quads-q
  "SELECT * WHERE { GRAPH ?c { ?s ?p ?o } VALUES ?c { %s } }")

(defn ds-quads [conn graphs]
  (let [q (format ds-quads-q (string/join " " (map #(str "<" % ">") graphs)))]
    (repo/query conn q)))

(defn get-draftset-quads [system draftset-id]
  (with-open [conn (-> system
                       :drafter.common.config/sparql-query-endpoint
                       repo/sparql-repo
                       repo/->connection)]
    (let [ds-graphs (keys (:changes (ops/get-draftset-info conn draftset-id)))
          draft-graphs (mapv (partial draft-graph-uri-for conn) ds-graphs)]
      (set (mapv (juxt :s :p :o :c) (ds-quads conn draft-graphs))))))

(defn get-live-quads-for-graphs [system graphs]
  (with-open [conn (-> system
                       :drafter.common.config/sparql-query-endpoint
                       repo/sparql-repo
                       repo/->connection)]
    (set (mapv (juxt :s :p :o :c) (ds-quads conn graphs)))))

(defn copy-live-graph-into-draftset [handler draftset-location draftset-id graph]
  (-> (tc/with-identity test-publisher
        {:uri (str draftset-location "/graph")
         :request-method :put
         :params {:draftset-id draftset-id
                  :graph (str graph)}})
      (handler)
      (get-in [:body :finished-job])
      (tc/await-success)))

(defn draftset-functions [system]
  (let [handler (get system [:drafter/routes :draftset/api])
        live    (:drafter.routes.sparql/live-sparql-query-route system)
        draftset-location (help/create-draftset-through-api handler test-publisher)
        draftset-id (last (string/split draftset-location #"/"))
        ->uri #(if (keyword? %) (URI. (str "http://" (name %))) %)
        ->quad #(apply ->Quad (map ->uri %))
        ->quads (partial map ->quad)]

    (letfn [(set-live! [quads]
              (let [draftset-location (help/create-draftset-through-api
                                       handler test-publisher)
                    quads (->quads quads)
                    graphs (distinct (map :c quads))]
                (help/append-quads-to-draftset-through-api
                 handler test-publisher draftset-location quads)
                (help/publish-draftset-through-api
                 handler draftset-location test-publisher)
                (get-live-quads-for-graphs system graphs)))

            (append! [quads]
              (help/append-quads-to-draftset-through-api
               handler test-publisher draftset-location (->quads quads))
              (get-draftset-quads system draftset-id))

            (delete! [quads]
              (help/delete-quads-through-api
               handler test-publisher draftset-location (->quads quads))
              (get-draftset-quads system draftset-id))

            (copy-graph! [graph]
              (copy-live-graph-into-draftset
               handler draftset-location draftset-id graph)
              (get-draftset-quads system draftset-id))

            (delete-graph! [graph]
              (help/delete-draftset-graph-through-api
               handler test-publisher draftset-location graph)
              (get-draftset-quads system draftset-id))

            (publish! [& live-graphs-to-return]
              (help/publish-draftset-through-api
               handler draftset-location test-publisher)
              (get-live-quads-for-graphs system live-graphs-to-return))

            (graph' [graph]
              (with-open [conn (-> system
                                   :drafter.common.config/sparql-query-endpoint
                                   repo/sparql-repo
                                   repo/->connection)]
                (draft-graph-uri-for conn graph)))]
      {:set-live! set-live!
       :append! append!
       :delete! delete!
       :copy-graph! copy-graph!
       :delete-graph! delete-graph!
       :publish! publish!
       :graph' (memoize graph')})))

(defmacro test-table [& forms]
  (->> forms
       (walk/postwalk
        (fn [x]
          (if (and (symbol? x) (= \' (last (name x))))
            (list 'graph' (symbol (apply str (butlast (name x)))))
            x)))
       (partition-by #{'|})
       (remove (partial every? #{'|}))
       (partition 4)
       (map (fn [[t action args expected]]
              `(let [t# (~@action ~@args)]
                 (is (= ~@expected t#)
                     ~(str "T" (first t) " | "(name (first action))
                           " did not match expected state")))))
       (cons 'do)))

(tc/deftest-system-with-keys sequence-1-test
  keys-for-test [system system-config]
  ;; From Rick's example
  ;; 1. Publish a graph :A with quad :foo :hasGraph :B :A in it, and the other
  ;;    quad :live-data :live-data :live-data :B in it.
  ;; 2. Create a new draftset
  ;; 3. PUT graph :A into our draftset (creating :foo :hasGraph :B :ADraft)
  ;; 4. PUT graph :B into our draftset (creating :live-data :live-data :live-data
  ;;    :BDraft) and updating `:foo :hasGraph :B :ADraft` to be:
  ;;    `:foo :hasGraph :BDraft :ADraft`
  ;; 5. Deleting graph B from draftset should result in :live-data :live-data
  ;;   :live-data :BDraft being removed AND :foo :hasGraph :BDraft :ADraft being
  ;;   updated back to :foo :hasGraph :B :ADraft
  (let [{:keys [set-live! append! delete! publish! copy-graph! delete-graph! graph']}
        (draftset-functions system)
        g1 (random-uri 'g)
        g2 (random-uri 'g)
        q1 [s1 p1 g2 g1]
        q2 [s2 p2 g1 g2]]
    ;; Trivial case Just appends no deletes:
    (test-table
     |;T | Operation     | Args     | Expected State                     |
     | 1 | set-live!     | #{q1 q2} | #{q1 q2}                           |
     | 2 | copy-graph!   | g1       | #{[s1 p1 g2 g1']}                  |
     | 3 | copy-graph!   | g2       | #{[s1 p1 g2' g1'] [s2 p2 g1' g2']} |
     | 4 | delete-graph! | g2       | #{[s1 p1 g2 g1']}                  |
     | 5 | publish!      | g1 g2    | #{q1}                              |)))

(tc/deftest-system-with-keys copy-live-graph-into-draftset-test
  keys-for-test [system system-config]
  (let [{:keys [set-live! append! delete! publish! copy-graph! graph']}
        (draftset-functions system)
        g1 (random-uri 'g)
        g2 (random-uri 'g)
        g3 (random-uri 'g)
        q1 [s1 p1 o1 g1]
        q2 [s2 p2 g1 g2]
        q3 [s3 p3 g2 g3]]
    ;; Trivial case Just appends no deletes:
    (test-table
     |;T | Operation   | Args     | Expected State                                    |
     | 1 | set-live!   | #{q1}    | #{q1}                                             |
     | 2 | append!     | #{q2}    | #{[s2 p2 g1 g2']}                                 |
     | 3 | append!     | #{q3}    | #{[s2 p2 g1 g2'] [s3 p3 g2' g3']}                 |
     | 4 | copy-graph! | g1       | #{[s1 p1 o1 g1'] [s2 p2 g1' g2'] [s3 p3 g2' g3']} |
     | 5 | publish!    | g1 g2 g3 | #{[s1 p1 o1 g1] [s2 p2 g1 g2] [s3 p3 g2 g3]}      |)))

;; Perfect Rewriting Scenarios

(tc/deftest-system-with-keys *_1_trivial-append-test
  keys-for-test [system system-config]
  ;; Trivial appends
  (let [{:keys [append! delete! publish! graph']} (draftset-functions system)
        g1  (random-uri 'g)
        g2  (random-uri 'g)
        q1  [s1 p1 o1 g1]
        q2  [s2 p2 g1 g2]]
    ;; Trivial case Just appends no deletes:
    (test-table
     |;T | Operation  | Args     | Expected State                    |
     |;1 | create-ds! | ...      | ...                               |
     | 2 | append!    | #{q1}    | #{[s1 p1 o1 g1']}                 |
     | 3 | append!    | #{q2}    | #{[s1 p1 o1 g1'] [s2 p2 g1' g2']} |
     | 4 | publish!   | g1 g2    | #{[s1 p1 o1 g1] [s2 p2 g1 g2]}    |)))

(tc/deftest-system-with-keys *_2_immediate-append-undo-test
  keys-for-test [system system-config]
  ;; Undoing an append immediately
  (let [{:keys [append! delete! publish! graph']} (draftset-functions system)
        g1  (random-uri 'g)
        g2  (random-uri 'g)
        q1  [s1 p1 o1 g1]
        q2  [s2 p2 g1 g2]]
    ;; Trivial case repeat of 1 but with delete of first quad, immediately after
    ;; first quads append:
    (test-table
     |;T | Operation  | Args     | Expected State    |
     |;1 | create-ds! | ...      | ...               |
     | 2 | append!    | #{q1}    | #{[s1 p1 o1 g1']} |
     | 3 | delete!    | #{q1}    | #{}               |
     | 4 | append!    | #{q2}    | #{[s2 p2 g1 g2']} |
     | 5 | publish!   | g1 g2    | #{[s2 p2 g1 g2 ]} |)))

(tc/deftest-system-with-keys *_3_undo-append-after-other-append-test
  keys-for-test [system system-config]
  ;; Undoing append later on
  (let [{:keys [append! delete! publish! graph']} (draftset-functions system)

        g1  (random-uri 'g)
        g2  (random-uri 'g)
        q1  [s1 p1 o1 g1]
        q2  [s2 p2 g1 g2]]
    ;; Similar to 2 except delete happens later on, g1' -> g1 when g1 is deleted
    (test-table
     |;T | Operation  | Args     | Expected State                    |
     |;1 | create-ds! | ...      | ...                               |
     | 2 | append!    | #{q1}    | #{[s1 p1 o1 g1']}                 |
     | 3 | append!    | #{q2}    | #{[s1 p1 o1 g1'] [s2 p2 g1' g2']} |
     | 4 | delete!    | #{q1}    | #{[s2 p2 g1 g2']}                 |
     | 5 | publish!   | g1 g2    | #{[s2 p2 g1 g2]}                  |)))

(tc/deftest-system-with-keys *_4_delete-without-graph-deletion-test
  keys-for-test [system system-config]
  ;; Delete without graph deletion
  (let [{:keys [append! delete! publish! graph']} (draftset-functions system)
        g1  (random-uri 'g)
        g2  (random-uri 'g)
        q1  [s1 p1 o1 g1]
        q2  [s1 p1 o2 g1]
        q3  [s2 p2 g1 g2]]
    ;; Similar to 2 but two quads in initial append, with only one deleted:
    (test-table
     |;T | Operation  | Args     | Expected State                                    |
     |;1 | create-ds! | ...      | ...                                               |
     | 2 | append!    | #{q1 q2} | #{[s1 p1 o1 g1'] [s1 p1 o2  g1']                } |
     | 3 | append!    | #{q3}    | #{[s1 p1 o1 g1'] [s1 p1 o2  g1'] [s2 p2 g1' g2']} |
     | 4 | delete!    | #{q1}    | #{[s1 p1 o2 g1'] [s2 p2 g1' g2']                } |
     | 5 | publish!   | g1 g2    | #{[s1 p1 o2 g1 ] [s2 p2 g1  g2 ]                } |)))

(tc/deftest-system-with-keys *_5_1_reference-live-graph-simple-test
  keys-for-test [system system-config]
  ;; Referencing live graph simple case
  (let [{:keys [set-live! append! delete! publish! graph']} (draftset-functions system)
        g1  (random-uri 'g)
        g2  (random-uri 'g)
        q1  [s1 p1 o1 g1]
        q2  [s2 p2 g1 g2]]
    ;; Similar to 1 but with a live graph referenced in draft
    (test-table
     |;T | Operation  | Args  | Expected State                  |
     | 1 | set-live!  | #{q1} | #{[s1 p1 o1 g1]}                |
     | 2 | append!    | #{q2} | #{[s2 p2 g1 g2']}               |
     | 3 | publish!   | g1 g2 | #{[s1 p1 o1 g1 ] [s2 p2 g1 g2]} |)))

(tc/deftest-system-with-keys *_5_2_deleting-triple-from-live-test
  keys-for-test [system system-config]
  ;; Deleting a triple from live
  (let [{:keys [set-live! append! delete! publish! graph']} (draftset-functions system)
        g1  (random-uri 'g)
        q1  [s1 p1 o1 g1]
        q2  [s2 p2 o2 g1]]
    ;; NOTE: Delete at T2 first copies g1 to g1' then applies delete (drafter
    ;; should do this already).
    (test-table
     |;T | Operation | Args     | Expected State    |
     | 1 | set-live! | #{q1 q2} | #{q1 q2}          |
     | 2 | delete!   | #{q2}    | #{[s1 p1 o1 g1']} |
     | 3 | publish!  | g1       | #{[s1 p1 o1 g1]}  |)))

(tc/deftest-system-with-keys *_5_3_reference-live-graph-with-append-test
  keys-for-test [system system-config]
  ;; referencing live graph with append on live graph
  (let [{:keys [set-live! append! delete! publish! graph']} (draftset-functions system)
        g1  (random-uri 'g)
        g2  (random-uri 'g)
        q1  [s1 p1 o1 g1]
        q2  [s1 p1 o2 g1]
        q3  [s2 p2 g1 g2]]
    ;; NOTE: At T2 we detect that we're modifying g1 so copy g1 into 'g1 and
    ;; then append [s1 p2 o2 g1].  Drafter should already do that copy.
    (test-table
     |;T | Operation     | Args  | Expected State                                   |
     | 1 | set-live!     | #{q1} | #{[s1 p1 o1 g1]}                                 |
     | 2 | append!       | #{q2} | #{[s1 p1 o1 g1'] [s1 p1 o2 g1']}                 |
     | 3 | append!       | #{q3} | #{[s1 p1 o1 g1'] [s1 p1 o2 g1'] [s2 p2 g1' g2']} |
     | 4 | publish!      | g1 g2 | #{[s1 p1 o1 g1] [s1 p1 o2 g1] [s2 p2 g1 g2]}     |)))


;; 6 Disambiguate deleting draftset changes from deleting live graph

(tc/deftest-system-with-keys *_6_1
  keys-for-test [system system-config]
  (let [{:keys [set-live! append! delete! delete-graph! publish! graph']}
        (draftset-functions system)
        g1 (random-uri 'g)]
    ;; At T3 we need to know that because we are starting with an empty g1
    ;; that we shouldn't first copy the graph into g1'.
    (test-table
     |;T | Operation     | Args                           | Expected State                 |
     | 1 | set-live!     | #{[s1 p1 o1 g1] [s2 p1 o1 g1]} | #{[s1 p1 o1 g1] [s2 p1 o1 g1]} |
     | 2 | delete-graph! | g1                             | #{}                            |
     | 3 | delete!       | [[:z1 :z1 :z1 g1]]             | #{}                            |
     | 4 | publish!      | g1                             | #{}                            |)))

(tc/deftest-system-with-keys *_6_2
  keys-for-test [system system-config]
  (let [{:keys [set-live! append! delete! delete-graph! publish! graph']}
        (draftset-functions system)
        g1  (random-uri 'g)
        q1  [s1 p1 o1 g1]
        q2  [s2 p1 o1 g1]]
    ;; In contrast to 6.1 at T2 we need to copy g1 into g1' then delete s1 p1 o1.
    (test-table
     |;T | Operation | Args    | Expected State                  |
     | 1 | set-live! | [q1 q2] | #{[s1 p1 o1 g1 ] [s2 p1 o1 g1]} |
     | 2 | delete!   | [q2]    | #{[s1 p1 o1 g1']              } |
     | 3 | publish!  | g1      | #{[s1 p1 o1 g1 ]              } |)))

(t/deftest *_6_3_delete-variants
  (tc/with-system
    keys-for-test [system system-config]
    ;; Delete variants
    (let [{:keys [set-live! append! delete! delete-graph! publish! graph']}
          (draftset-functions system)
          g1 (random-uri 'g)
          g2 (random-uri 'g)
          g3 (random-uri 'g)
          g4 (random-uri 'g)
          q1 [s1 p1 o1 g1]
          q2 [s2 p1 o1 g2]]
      ;; T2 deleting a live graph by supplying all its triples
      ;; T3 deleting a live graph through the delete graph op
      ;; T5 deleting a graph (created in T4) that only existed in the draft by
      ;; supplying triples
      ;; T7 deleting a graph (created in T6) that only existed in the draft
      ;; through the delete graph op
      (test-table
        |                                                   ;T | Operation     | Args             | Expected State    |
        | 1 | set-live! | #{q1 q2} | #{q1 q2} |
        | 2 | delete! | #{q1} | #{} |
        | 3 | delete-graph! | g2 | #{} |
        | 4 | append! | #{[s3 p3 o3 g3]} | #{[s3 p3 o3 g3']} |
        | 5 | delete! | #{[s3 p3 o3 g3]} | #{} |
        | 6 | append! | #{[s4 p4 o4 g4]} | #{[s4 p4 o4 g4']} |
        | 7 | delete-graph! | g4 | #{} |
        | 8 | publish! | g1 g2 g3 g4 | #{} |))))
