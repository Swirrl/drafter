(ns drafter.rewrite-batch-failure-test
  (:require [clojure.test :as t :refer [are is testing]]
            [grafter-2.rdf.protocols :as pr]
            [drafter.test-common :as tc]
            [drafter.user-test :refer [test-publisher]]
            [clojure.java.io :as io]
            [drafter.feature.draftset.test-helper :as help]
            [clojure.string :as string]
            [grafter-2.rdf4j.repository :as repo]
            [drafter.backend.draftset.operations :as ops]
            [drafter.draftset :as ds])
  (:import java.net.URI
           java.io.ByteArrayInputStream
           java.util.UUID))

(t/use-fixtures :each tc/with-spec-instrumentation)

(def system-config "test-system.edn")

(def keys-for-test
  [[:drafter/routes :draftset/api]
   :drafter/write-scheduler
   :drafter.routes.sparql/live-sparql-query-route
   :drafter.backend.live/endpoint
   :drafter.common.config/sparql-query-endpoint])

(def batch-size 50)

(defn uri-str [& strs]
  (URI. (apply str strs)))

(def valid-triples
  (let [g (UUID/randomUUID)]
    (->> (range)
         (map (fn [_]
                (let [uuid (UUID/randomUUID)]
                  (pr/->Quad
                   (uri-str "http://s/" uuid)
                   (uri-str "http://p/" 0)
                   (uri-str "http://g/" g)
                   (uri-str "http://g/" uuid)))))
         (cons (pr/->Quad
                (uri-str "http://s/" 0)
                (uri-str "http://p/" 0)
                (uri-str "http://g/" g)
                (uri-str "http://g/" g))))))

(def invalid-triples-str
  (with-open [r (java.io.StringWriter.)]
    (pr/add (grafter-2.rdf4j.io/rdf-writer r :format :nq)
            (take 60 valid-triples))
    (str r "<http://s/10> <http://p/10/ > \"test\" <http://g/graph> .")))

(def dg-q
  "SELECT ?o WHERE { GRAPH ?g { <%s> <http://publishmydata.com/def/drafter/hasDraft> ?o } }")

(defn draft-graph-uri-for [conn graph-uri]
  (:o (first (repo/query conn (format dg-q graph-uri)))))

(def ds-quads-q
  "SELECT * WHERE { GRAPH ?c { ?s ?p ?o } VALUES ?c { %s } }")

(defn ds-quads [conn graphs]
  (let [q (format ds-quads-q (string/join " " (map #(str "<" % ">") graphs)))]
    (repo/query conn q)))

(defn- get-user-draftset-quads [system draftset-id]
  (with-open [conn (-> system
                       :drafter.common.config/sparql-query-endpoint
                       repo/sparql-repo
                       repo/->connection)]
    (let [system-draftset-info (ops/get-draftset-info conn draftset-id)
          draftset-info (help/user-draftset-info-view system-draftset-info)
          ds-graphs (keys (:changes draftset-info))
          draft-graphs (mapv (partial draft-graph-uri-for conn) ds-graphs)]
      (set (ds-quads conn draft-graphs)))))

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

(t/deftest bad-triple-in-last-batch
  (tc/with-system
    keys-for-test [system system-config]
    (with-redefs [drafter.rdf.draftset-management.job-util/batched-write-size 5]
      (let [handler (get system [:drafter/routes :draftset/api])
            draftset-location (help/create-draftset-through-api handler test-publisher)
            draftset-id (last (string/split draftset-location #"/"))
            input-stream (ByteArrayInputStream. (.getBytes invalid-triples-str))
            request (help/append-to-draftset-request
                      test-publisher draftset-location input-stream
                      {:content-type "application/n-quads"})
            response (handler request)
            complete (tc/await-completion (get-in response [:body :finished-job]))
            mapping (with-open [conn (-> system
                                         :drafter.common.config/sparql-query-endpoint
                                         repo/sparql-repo
                                         repo/->connection)]
                      (ds-mapping conn draftset-id))
            quads (get-user-draftset-quads system draftset-id)]
        (is (= {:type        :error
                :message     "Reading triples aborted."
                :error-class "clojure.lang.ExceptionInfo"
                :details     {:error :reading-aborted}}
               complete))
        (is (= 60 (count quads)))
        (is (= quads (set (rewrite (take 60 valid-triples) mapping))))))))
