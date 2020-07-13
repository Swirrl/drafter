(ns drafter.rewrite-consistency-test
  (:require [clojure.test :as t :refer [are is testing]]
            [clojure.data]
            [grafter-2.rdf.protocols :as pr]
            [drafter.test-common :as tc]
            [drafter.user-test :refer [test-publisher]]
            [clojure.java.io :as io]
            [drafter.feature.draftset.test-helper :as help]
            [clojure.string :as string]
            [grafter-2.rdf4j.repository :as repo]
            [drafter.backend.draftset.operations :as ops]
            [clojure.set :as set]
            [drafter.draftset :as ds]
            [drafter.backend.draftset :as ep]
            [drafter.middleware :as mw]
            [drafter.backend.draftset.operations :as dsops]
            [drafter.rdf.sparql-protocol :as sp]
            [drafter.backend :as backend])
  (:import java.net.URI)
  )

(t/use-fixtures :each tc/with-spec-instrumentation)

(def system-config "test-system.edn")

(def keys-for-test
  [[:drafter/routes :draftset/api]
   :drafter/backend
   :drafter/write-scheduler
   :drafter.routes.sparql/live-sparql-query-route
   :drafter.backend.live/endpoint
   :drafter.common.config/sparql-query-endpoint])

(def batch-size 50)

(defn uri-str [& strs]
  (URI. (apply str strs)))

(def valid-triples
  (->> (range)
       (map (fn [i]
                 (pr/->Quad
                  (uri-str "http://s/" i)
                  (uri-str "http://p/" i)
                  (uri-str "http://o/" i)
                  (uri-str "http://g/graph"))))))

(defn quad [g pos i]
  (pr/->Quad
   (if (= pos :s) g (uri-str "http://s/" i))
   (if (= pos :p) g (uri-str "http://p/" i))
   (if (= pos :o) g (uri-str "http://o/" i))
   g))

(defn graph-referencing-triples [start graphs]
  (->> graphs
       (map-indexed (fn [i g]
                      (let [j (+ i start)]
                        [(quad g :s j) (quad g :p j) (quad g :o j)])))
       (apply concat)))

(def dg-q
  "SELECT ?o WHERE { GRAPH ?g { <%s> <http://publishmydata.com/def/drafter/hasDraft> ?o } }")

(defn draft-graph-uri-for [conn graph-uri]
  (:o (first (repo/query conn (format dg-q graph-uri)))))

(def ds-quads-q
  "
SELECT ?c ?s ?p ?o WHERE {
  GRAPH ?c { ?s ?p ?o }
  GRAPH <http://publishmydata.com/graphs/drafter/drafts> {
    VALUES ?ds { <%s> }
    ?ds <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://publishmydata.com/def/drafter/DraftSet> .
    ?c <http://publishmydata.com/def/drafter/inDraftSet> ?ds .
    ?l <http://publishmydata.com/def/drafter/hasDraft> ?c .
  }
}")

(defn get-draftset-quads [system draftset-id]
  (let [draftset-uri (ds/->draftset-uri draftset-id)
        q (format ds-quads-q draftset-uri)]
    (with-open [conn (-> system
                         :drafter.common.config/sparql-query-endpoint
                         repo/sparql-repo
                         repo/->connection)]
      (set (repo/query conn q)))))

(defn rewritten? [quads draft-graph-uri]
  (every? #(= draft-graph-uri (nth % 2)) quads))

(defn rewrite [quads mapping]
  (letfn [(rewrite1 [x] (get mapping x x))]
    (map (fn [q]
           (-> (into {} q)
               (update :s rewrite1)
               (update :p rewrite1)
               (update :o rewrite1)
               (update :c rewrite1)))
         quads)))

(def ds-mapping-q
  "
SELECT ?dg ?lg WHERE {
  GRAPH <http://publishmydata.com/graphs/drafter/drafts> {
    BIND ( <%s> AS ?ds )
    ?ds <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>
        <http://publishmydata.com/def/drafter/DraftSet> .
    ?dg <http://publishmydata.com/def/drafter/inDraftSet> ?ds .
    ?lg <http://publishmydata.com/def/drafter/hasDraft> ?dg .
  }
}")

(defn ds-mapping [conn draftset-id]
  (->> (repo/query conn (format ds-mapping-q (ds/->draftset-uri draftset-id)))
       (mapv (juxt :lg :dg))
       (into {})))

;; Make this number bigger if you want to check consistency of large numbers of
;; quads. E.G., larger than the current 75k batch size. This will take a while
;; though.
(def n-valid 10)

(tc/deftest-system-with-keys append-consistency-test
  keys-for-test [system system-config]
  (let [handler (get system [:drafter/routes :draftset/api])
        draftset-location (help/create-draftset-through-api handler test-publisher)
        draftset-id (last (string/split draftset-location #"/"))
        n-graphs 10
        graphs (->> (range n-valid (+ n-graphs n-valid))
                    (map (fn [i] (uri-str "http://g/" i)))
                    (take n-graphs))
        grefs (graph-referencing-triples n-valid graphs)
        quads (shuffle (distinct (concat (take n-valid valid-triples) grefs)))
        response (help/append-quads-to-draftset-through-api
                  handler test-publisher draftset-location quads)
        mapping (with-open [conn (-> system
                                     :drafter.common.config/sparql-query-endpoint
                                     repo/sparql-repo
                                     repo/->connection)]
                  (ds-mapping conn draftset-id))
        quads' (get-draftset-quads system draftset-id)
        quads'' (set (rewrite quads mapping))]
    (is (= quads' quads''))))

(tc/deftest-system-with-keys delete-consistency-test
  keys-for-test [system system-config]
  (let [handler (get system [:drafter/routes :draftset/api])
        draftset-location (help/create-draftset-through-api handler test-publisher)
        draftset-id (last (string/split draftset-location #"/"))
        n-graphs 10
        graphs (->> (range n-valid (+ n-graphs n-valid))
                    (map (fn [i] (uri-str "http://g/" i)))
                    (take n-graphs))
        quads (shuffle (concat (take n-valid valid-triples)
                               (graph-referencing-triples n-valid graphs)))
        to-delete (take 20 quads)
        _ (help/append-quads-to-draftset-through-api
           handler test-publisher draftset-location quads)
        _ (help/delete-quads-through-api
           handler test-publisher draftset-location to-delete)
        mapping (with-open [conn (-> system
                                     :drafter.common.config/sparql-query-endpoint
                                     repo/sparql-repo
                                     repo/->connection)]
                  (ds-mapping conn draftset-id))
        quads' (get-draftset-quads system draftset-id)
        quads'' (-> (set quads)
                    (set/difference (set to-delete))
                    (rewrite mapping)
                    (set))]
    (is (= quads' quads''))))

(tc/deftest-system-with-keys delete-graph-consistency-test
  keys-for-test [system system-config]
  (let [handler (get system [:drafter/routes :draftset/api])
        draftset-location (help/create-draftset-through-api handler test-publisher)
        draftset-id (last (string/split draftset-location #"/"))
        n-graphs 10
        graphs (->> (range n-valid (+ n-graphs n-valid))
                    (map (fn [i] (uri-str "http://g/" i)))
                    (take n-graphs))
        quads (shuffle (concat (take n-valid valid-triples)
                               (graph-referencing-triples n-valid graphs)))
        to-delete (rand-nth graphs)
        _ (help/append-quads-to-draftset-through-api
           handler test-publisher draftset-location quads)
        _ (help/delete-draftset-graph-through-api
           handler test-publisher draftset-location to-delete)
        mapping (with-open [conn (-> system
                                     :drafter.common.config/sparql-query-endpoint
                                     repo/sparql-repo
                                     repo/->connection)]
                  (ds-mapping conn draftset-id))
        quads' (get-draftset-quads system draftset-id)
        quads'' (-> (remove #(= to-delete (:c %)) quads)
                    (rewrite mapping)
                    (set))]
    (is (= quads' quads''))))

(defn copy-live-graph-into-draftset [handler draftset-location draftset-id graph]
  (-> (tc/with-identity test-publisher
        {:uri (str draftset-location "/graph")
         :request-method :put
         :params {:draftset-id draftset-id
                  :graph (str graph)}})
      (handler)
      (get-in [:body :finished-job])
      (tc/await-success)))

(tc/deftest-system-with-keys copy-graph-consistency-test
  keys-for-test [system system-config]
  (let [handler (get system [:drafter/routes :draftset/api])
        draftset-location (help/create-draftset-through-api handler test-publisher)
        draftset-id (last (string/split draftset-location #"/"))
        n-graphs 10
        graphs (->> (range n-valid (+ n-graphs n-valid))
                    (map (fn [i] (uri-str "http://g/" i)))
                    (take n-graphs))
        quads (shuffle (concat (take n-valid valid-triples)
                               (graph-referencing-triples n-valid graphs)))
        to-copy (rand-nth graphs)
        _ (help/append-quads-to-draftset-through-api
           handler test-publisher draftset-location quads)
        _ (help/publish-draftset-through-api
           handler draftset-location test-publisher)
        draftset-location (help/create-draftset-through-api handler test-publisher)
        draftset-id (last (string/split draftset-location #"/"))
        _ (copy-live-graph-into-draftset
           handler draftset-location draftset-id to-copy)
        mapping (with-open [conn (-> system
                                     :drafter.common.config/sparql-query-endpoint
                                     repo/sparql-repo
                                     repo/->connection)]
                  (ds-mapping conn draftset-id))
        quads' (get-draftset-quads system draftset-id)
        quads'' (-> (filter #(= to-copy (:c %)) quads)
                    (rewrite mapping)
                    (set))]
    (is (= quads' quads''))

    ;; clean up
    (help/delete-quads-through-api
     handler test-publisher draftset-location quads)
    (help/publish-draftset-through-api
     handler draftset-location test-publisher)))

(tc/deftest-system-with-keys copy-graph-append-consistency-test
  keys-for-test [system system-config]
  (let [handler (get system [:drafter/routes :draftset/api])

        graph (uri-str "http://g/" 0)
        quads0 [(pr/->Quad (uri-str "http://s/" 0)
                           (uri-str "http://p/" 0)
                           (uri-str "http://o/" 0)
                           graph)]

        ;; 1st draftset to get triples into live
        draftset-location (help/create-draftset-through-api handler test-publisher)
        draftset-id (last (string/split draftset-location #"/"))
        _ (help/append-quads-to-draftset-through-api
           handler test-publisher draftset-location quads0)
        _ (help/publish-draftset-through-api
           handler draftset-location test-publisher)

        quads1 [(pr/->Quad (URI. "http://s/1")
                           (URI. "http://p/1")
                           (URI. "http://o/1")
                           graph)]

        ;; yet another draftset
        draftset-location (help/create-draftset-through-api handler test-publisher)
        draftset-id (last (string/split draftset-location #"/"))

        ;; ensure draftset is currently empty
        _ (is (empty? (get-draftset-quads system draftset-id)))

        response (help/append-quads-to-draftset-through-api
                  handler test-publisher draftset-location quads1)

        quads' (get-draftset-quads system draftset-id)

        mapping (with-open [conn (-> system
                                     :drafter.common.config/sparql-query-endpoint
                                     repo/sparql-repo
                                     repo/->connection)]
                  (ds-mapping conn draftset-id))

        quads'' (set (rewrite (set/union quads0 quads1) mapping))]
    (is (= quads'' quads'))))
