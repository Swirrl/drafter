(ns drafter.stasher-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t]
            [drafter.stasher :as sut]
            [drafter.stasher.filecache :as fc]
            [drafter.test-common :as tc :refer [with-system deftest-system]]
            [grafter-2.rdf.protocols :as pr]
            [grafter-2.rdf4j.io :as rio]
            [grafter-2.rdf4j.repository :as repo]
            [drafter.stasher.formats :as formats]
            [grafter.url :as url]
            [integrant.core :as ig]
            [me.raynes.fs :as fs]
            [grafter.vocabularies.rdf :refer [rdfs:label]]
            [grafter-2.rdf4j.sparql :as sp])
  (:import [java.net URI]
           org.eclipse.rdf4j.model.impl.URIImpl
           org.eclipse.rdf4j.query.impl.SimpleDataset
           (org.eclipse.rdf4j.query.parser ParsedGraphQuery ParsedTupleQuery ParsedBooleanQuery)
           (org.eclipse.rdf4j.query BooleanQuery TupleQuery GraphQuery)
           (org.eclipse.rdf4j.query.parser.sparql SPARQLParser)
           org.eclipse.rdf4j.query.resultio.QueryResultIO
           org.eclipse.rdf4j.rio.RDFHandler
           (org.eclipse.rdf4j.query TupleQueryResult TupleQueryResultHandler)
           org.eclipse.rdf4j.query.resultio.TupleQueryResultWriter
           java.time.OffsetDateTime))

(t/use-fixtures :each tc/with-spec-instrumentation)

(def test-triples [(pr/->Quad (URI. "http://foo") (URI."http://is/a") (URI."http://is/triple") nil)])

(defmethod ig/init-key ::stub-cache-key-generator [_ opts]
  sut/generate-drafter-cache-key)

(def fixed-time "A fixed time to ensure queries are cached with a known cache key (for testing)"
  (java.time.OffsetDateTime/parse "2019-01-25T01:01:01Z"))

(defn- sneak-rdf-file-into-cache!
  "Force the creation of an entry in the cache via the backdoor "
  [cache repo dataset query-string]
  (let [cache-key (with-open [conn (.getConnection repo)]
                    (sut/generate-drafter-cache-key fixed-time :graph cache query-string dataset conn))
        fmt (get-in cache [:formats :graph])]
    (with-open [in-stream (fc/destination-stream (:cache-backend cache)
                                                 cache-key
                                                 fmt)]
      (let [writer (rio/rdf-writer in-stream :format fmt)]
        (pr/add writer test-triples)
        (.endRDF writer)))))

(def basic-construct-query "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o . }")

(deftest-system stasher-repo-return-cache-hit-test
  [{:keys [drafter.stasher/repo drafter.stasher/cache]}
   "drafter/stasher-test/stasher-repo-return-cache-hit-test.edn"]
  (t/testing "Querying a cached value returns the cached RDF"
    (let [dataset (repo/make-restricted-dataset :default-graph "http://fake-graph.com/")
          query-str basic-construct-query]

      (sneak-rdf-file-into-cache! cache repo dataset query-str)

      (t/is (= test-triples
               (repo/query (repo/->connection repo) query-str :default-graph "http://fake-graph.com/"))))))


(defmulti parse-query-type type)

(defmethod parse-query-type String [sparql-query-string]
  (condp instance? (.parseQuery (SPARQLParser.) sparql-query-string nil)
    ParsedBooleanQuery :boolean
    ParsedGraphQuery :graph
    ParsedTupleQuery :tuple))

(defn box-result [res]
  (if (boolean? res)
    [res]
    (doall res)))

(defn binding-set->grafter-type [binding-set]
  (let [binding-names (.getBindingNames binding-set)]
    (zipmap (map keyword binding-names)
            (map #(-> binding-set (.getBinding %) .getValue pr/->grafter-type) binding-names))))

(defn assert-cached-results
  [cache raw-repo query dataset expected-data]
  ;; evidence that we didn't just run another uncached query
  (let [query-type (parse-query-type query)
        dir (get-in cache [:cache-backend :dir])
        fmt (get-in cache [:formats query-type])
        cache-key (with-open [conn (.getConnection raw-repo)]
                    (sut/generate-drafter-cache-key fixed-time query-type cache query dataset conn))
        cached-file (fc/cache-key->cache-path dir fmt cache-key)]
    (t/testing "Prove the side-effect of creating the file in the cache happened"
      (t/is (.exists cached-file)
            (format "File %s does not exist" cached-file)))

    (t/testing "Prove the file that was written to the cache is the same as the fixture data that went in"
      (when (fs/exists? cached-file)
        (let [cached-file-statements (let [stream (io/input-stream cached-file)]
                                       (condp = query-type
                                         :tuple (let [tq-res (QueryResultIO/parseTupleBackground stream (get-in formats/supported-cache-formats [:tuple fmt]))]
                                                  (iterator-seq (reify java.util.Iterator
                                                                  (next [this]
                                                                    (let [binding-set (.next tq-res)]
                                                                      (binding-set->grafter-type binding-set)))
                                                                  (hasNext [this]
                                                                    (.hasNext tq-res)))))
                                         :graph (rio/statements stream :format (get-in formats/supported-cache-formats [:graph fmt]))
                                         :boolean (let [bqrp (QueryResultIO/createBooleanParser (get-in formats/supported-cache-formats [:boolean fmt]))]
                                                    (.parse bqrp stream))))]
          (t/is (= (set (box-result cached-file-statements))
                   (set (box-result expected-data)))))))))



(defn assert-caches-query [cached-repo cache query-str expected-data]
  (t/testing "Cached & uncached queries return expected data and expected data is stored in the cache"
    (with-open [cached-conn (repo/->connection cached-repo)]
      (let [uncached-results (box-result (repo/query cached-conn query-str)) ;; first query should be uncached
            cached-results (box-result (repo/query cached-conn query-str))
            dataset (repo/make-restricted-dataset)]

        (t/testing "The cached, uncached and fixture data are all the same"
          (t/is (= (set uncached-results)
                   (set cached-results)
                   (set (box-result expected-data)))))

        (t/testing "Results for query are stored on disk"
          ;; Check that the expected fixture data was stored in the cache on disk
          ;; TODO Dan: I've commented this out because stasher now generates a
          ;; TODO Dan: cache key containing the state graph modified time, and
          ;; TODO Dan: we don't have access to that here to recreate the same
          ;; TODO Dan: key.
          ;; TODO Dan: Long term, I'd like to split stasher / repo / filecache
          ;; TODO Dan: tests and remove all disk checks from here
          ;; (assert-cached-results cache uncached-repo query-str dataset expected-data)
          )))))

(deftest-system stasher-queries-pull-test
  [{caching-repo :drafter.stasher/repo
    cache :drafter.stasher/cache
    {:keys [fixtures]} :drafter.fixture-data/loader}
   "drafter/stasher-test/stasher-repo-cache-and-serve-test.edn"]
  (t/testing "Stashing of all query types"
    (t/testing "ASK"
      (assert-caches-query caching-repo cache "ASK WHERE { ?s ?p ?o }" #{true})
      (assert-caches-query caching-repo cache "ASK WHERE { <http://not> <http://in> <http://db> }" #{false}))

    ;; (t/testing "SELECT"
    ;;   (let [select-query "SELECT * WHERE { ?s ?p ?o } LIMIT 2"
    ;;         expected-data (with-open [uncconn (repo/->connection uncached-repo)]
    ;;                         (doall (repo/query uncconn select-query)))]
    ;;     (assert-caches-query caching-repo uncached-repo cache select-query expected-data)))

    ;; (let [graph-data (rio/statements (first fixtures) :format :ttl)]
    ;;   (t/testing "CONSTRUCT"
    ;;     (assert-caches-query caching-repo uncached-repo cache basic-construct-query graph-data))

    ;;   (t/testing "DESCRIBE"
    ;;     (assert-caches-query caching-repo uncached-repo cache "DESCRIBE <http://statistics.gov.scot/data/home-care-clients>" graph-data)))
    ))

(defn recording-rdf-handler
  "Convenience function that returns a 2-tuple of a recorded events
  atom and an rdf-handler that will record events inside an atom.

  The first item in the tuple is the atom, the second the
  rdf-handler."
  []
  (let [recorded-events (atom {})
        rdf-handler (reify RDFHandler
                      (startRDF [this]
                        (swap! recorded-events assoc :started true))
                      (endRDF [this]
                        (swap! recorded-events assoc :ended true))
                      (handleStatement [this statement]
                        (swap! recorded-events update :data conj (rio/backend-quad->grafter-quad statement)))
                      (handleComment [this comment]
                        (swap! recorded-events update :comments conj comment))
                      (handleNamespace [this prefix uri]
                        (swap! recorded-events update :namespaces assoc prefix uri)))]

    [recorded-events rdf-handler]))

(defn recording-tuple-handler
  "Convenience function that returns a 2-tuple of recorded-events and
  a tuple-handler that will record events inside an atom.

  The first item in the tuple is the atom, the second the
  rdf-handler."
  []
  (let [recorded-events (atom {})
        tuple-handler (reify
                        TupleQueryResult
                        (getBindingNames [this]
                          ;; Hardcoded as a stub for these specific tests
                          ["s" "p" "o"])
                        (close [this]
                          )
                        TupleQueryResultHandler
                        (startQueryResult [this binding-names]
                          (swap! recorded-events assoc
                                 :started true
                                 :binding-names binding-names))
                        (endQueryResult [this]
                          (swap! recorded-events assoc :ended true))
                        (handleSolution [this binding-set]
                          (swap! recorded-events update :data conj (binding-set->grafter-type binding-set)))
                        (handleLinks [this link-urls]
                          (swap! recorded-events update :links conj link-urls)))]

    [recorded-events tuple-handler]))

(defn- assert-same-cached-uncached
  [query-type preped-query cache raw-repo fixtures make-recording-handler]
  (t/testing "A cache miss returns the results and stores them in the cache"
    (let [[uncached-recorded-events uncached-event-handler] (make-recording-handler)
          [cached-recorded-events cached-event-handler] (make-recording-handler)]

      (.evaluate preped-query uncached-event-handler) ;; run uncached query
      (.evaluate preped-query cached-event-handler) ;; run again (the cached query)

      (t/is (= true
               (:started @uncached-recorded-events)
               (:started @cached-recorded-events))
            "startRDF called")

      (t/is (= true
               (:ended @uncached-recorded-events)
               (:ended @cached-recorded-events))
            "endRDF called")

      (t/is (= @uncached-recorded-events
               @cached-recorded-events)
            "fires same set of events whether cached or not")

      (t/testing "The cached, uncached queries return the same data"
        (t/is (= (set (:data @uncached-recorded-events))
                 (set (:data @cached-recorded-events))))))))

#_(deftest-system stasher-queries-push-test
  [{repo :drafter.stasher/repo
    cache :drafter.stasher/cache
    raw-repo :drafter.backend/rdf4j-repo
    {:keys [fixtures]} :drafter.fixture-data/loader} "drafter/stasher-test/stasher-repo-cache-and-serve-test.edn"]

  (t/testing "Push interface to query results"
    (with-open [conn (repo/->connection repo)]
      (t/testing "CONSTRUCT (RDFHandler)"
             (let [construct-query (.prepareGraphQuery conn basic-construct-query)]
               (assert-same-cached-uncached :graph construct-query cache raw-repo fixtures recording-rdf-handler))
             (let [cache-key (sut/generate-drafter-cache-key :graph cache basic-construct-query nil {:raw-repo raw-repo})
                   dir (get-in cache [:cache-backend :dir])
                   fmt (get-in cache [:formats :graph])
                   cached-file (fc/cache-key->cache-path dir fmt cache-key)
                   cached-file-statements (-> cached-file
                                              io/input-stream
                                              (rio/statements :format (get-in formats/supported-cache-formats [:graph fmt])))]
               (assert-cached-results cache raw-repo basic-construct-query nil cached-file-statements)))

      (t/testing "SELECT (TupleResultHandler)"
        (let [simple-select-query "SELECT * WHERE { ?s ?p ?o }"
              tuple-query (.prepareTupleQuery conn simple-select-query)]
          (assert-same-cached-uncached :tuple tuple-query cache raw-repo fixtures recording-tuple-handler)

          ;; run the same select query on a fixture repo to get the data in the expected format
          (let [expected-results (with-open [conn (-> fixtures
                                                      first
                                                      rio/statements
                                                      repo/fixture-repo
                                                      repo/->connection)]
                                   (doall (repo/query conn simple-select-query)))]
            (assert-cached-results cache raw-repo simple-select-query nil expected-results))))

      ;; NOTE there are no tests for push boolean results, because
      ;; that .evaluate interface doesn't exist in RDF4j.
      )))

(t/deftest dataset->graph-edn-test
  (let [ds-1 (doto (SimpleDataset.)
               (.addNamedGraph (URIImpl. "http://foo"))
               (.addNamedGraph (URIImpl. "http://bar"))
               (.addDefaultGraph (URIImpl. "http://foo"))
               (.addDefaultGraph (URIImpl. "http://bar")))
        ds-2 (doto (SimpleDataset.)
               (.addNamedGraph (URIImpl. "http://bar"))
               (.addDefaultGraph (URIImpl. "http://bar"))
               (.addNamedGraph (URIImpl. "http://foo"))
               (.addDefaultGraph (URIImpl. "http://foo")))]

    (t/testing "dataset->graphs, graphs->edn prints same value irrespective of assembly order"
      (t/is (= (pr-str (sut/graphs->edn (sut/dataset->graphs ds-1)))
               (pr-str (sut/graphs->edn (sut/dataset->graphs ds-2))))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Drafter State Graph Stashing
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Definitions corresponding to idenfitiers/states in drafter-state-1.trig
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def live-graph-1 (URIImpl. "http://live-and-ds1-and-ds2"))
(def live-graph-only (URIImpl. "http://live-only"))

(def liveset-most-recently-modified {:livemod (OffsetDateTime/parse "2017-02-02T02:02:02.000-00:00")})

(def ds-1-dg-1 (URIImpl. "http://publishmydata.com/graphs/drafter/draft/ds-1-dg-1"))
(def ds-1-dg-2 (URIImpl. "http://publishmydata.com/graphs/drafter/draft/ds-1-dg-2"))
(def ds-1 "draftset-1 is made of dg-1 dg-2" #{ds-1-dg-1 ds-1-dg-2})

(def ds-1-most-recently-modified "modified time of dg-2 the most recently modified graph in ds1"
  {:draftmod (OffsetDateTime/parse "2017-04-04T04:04:04.000-00:00")})


(def ds-2-dg-1 (URIImpl. "http://publishmydata.com/graphs/drafter/draft/ds-2-dg-1"))

(def ds-2 #{ds-2-dg-1})

(def ds-2-most-recently-modified "modified time of dg-2 the most recently modified graph in ds1"
  {:draftmod (OffsetDateTime/parse "2017-05-05T05:05:05.000-00:00")})


(defn edn->dataset [{:keys [default-graphs named-graphs]}]
  (reduce (fn [dataset graph]
            (doto dataset
              (.addDefaultGraph (if (instance? org.eclipse.rdf4j.model.URI graph)
                                  graph
                                  (URIImpl. graph)))
              (.addNamedGraph (if (instance? org.eclipse.rdf4j.model.URI graph)
                                  graph
                                  (URIImpl. graph)))))
          (SimpleDataset.)
          (concat default-graphs named-graphs)))


(comment


  ;;; TODO TODO TODO TODO


  (def repo (grafter-2.rdf4j.repository/resource-repo "drafter/stasher-test/drafter-state-1.trig" ))
  ;(def repo (grafter-2.rdf4j.repository/sparql-repo "http://localhost:5820/drafter-test-db/query"))

  (sut/fetch-modified-state (grafter-2.rdf4j.repository/->connection repo)
                            {:named-graphs ds-1 :default-graphs ds-1})

  )

(deftest-system fetch-modified-state-test
  [{repo :drafter.stasher/repo} "drafter/stasher-test/drafter-state-1.edn"]
  (t/is (some? repo) "No repo!")
  (with-open [conn (.getConnection repo)]

    (t/testing "Fetching draftset modified times"
      (t/is (= ds-1-most-recently-modified
               (sut/fetch-modified-state conn {:named-graphs ds-1 :default-graphs ds-1})))

      (t/is (= ds-2-most-recently-modified
               (sut/fetch-modified-state conn {:named-graphs ds-2 :default-graphs ds-2}))))

    (t/testing "Fetching live graph modified times"
      (t/is (= liveset-most-recently-modified
               (sut/fetch-modified-state conn {:named-graphs [live-graph-1 live-graph-only] :default-graphs [live-graph-1 live-graph-only]}))))

    (t/testing "Fetching draftsets with union-with-live set"
      ;; Union with live is at this low level equivalent to merging the
      ;; set of live graphs in to :named-graphs and :default-graphs.
      (let [ds-1-union-with-live (conj ds-1 live-graph-1 live-graph-only)
            dataset {:named-graphs ds-1-union-with-live :default-graphs ds-1-union-with-live}]
        (t/is (= (merge liveset-most-recently-modified
                        ds-1-most-recently-modified)
                 (sut/fetch-modified-state conn dataset))))))
  (with-open [conn (.getConnection repo)]

    (t/testing "Fetching draftset modified times"
      (t/is (= ds-1-most-recently-modified
               (sut/fetch-modified-state conn {:named-graphs ds-1 :default-graphs ds-1})))

      (t/is (= ds-2-most-recently-modified
               (sut/fetch-modified-state conn {:named-graphs ds-2 :default-graphs ds-2}))))

    (t/testing "Fetching live graph modified times"
      (t/is (= liveset-most-recently-modified
               (sut/fetch-modified-state conn {:named-graphs [live-graph-1 live-graph-only] :default-graphs [live-graph-1 live-graph-only]}))))

    (t/testing "Fetching draftsets with union-with-live set"
      ;; Union with live is at this low level equivalent to merging the
      ;; set of live graphs in to :named-graphs and :default-graphs.
      (let [ds-1-union-with-live (conj ds-1 live-graph-1 live-graph-only)
            dataset {:named-graphs ds-1-union-with-live :default-graphs ds-1-union-with-live}]
        (t/is (= (merge liveset-most-recently-modified
                        ds-1-most-recently-modified)
                 (sut/fetch-modified-state conn dataset)))))))

(deftest-system generate-drafter-cache-key-test
  [{:keys [drafter.stasher/repo
           drafter.stasher/filecache]} "drafter/stasher-test/drafter-state-1.edn"]

  (let [dataset (edn->dataset {:named-graphs [live-graph-1 live-graph-only]
                               :default-graphs [live-graph-1 live-graph-only]})
        result (with-open [conn (.getConnection repo)]
                 (sut/generate-drafter-cache-key fixed-time :graph filecache basic-construct-query dataset conn))]

    (let [{:keys [dataset query-str modified-times]} result]
      (t/is (=
             {:default-graphs #{"http://live-and-ds1-and-ds2" "http://live-only"},
              :named-graphs #{"http://live-and-ds1-and-ds2" "http://live-only"}}
             dataset))
      (t/is (= basic-construct-query
               query-str))
      (t/is (= modified-times
               {:livemod (OffsetDateTime/parse "2017-02-02T02:02:02.000-00:00")})))))

(defn- prepare-query
  "Prepares an RDF4j query from a connection with the specified bindings set"
  ([conn query-string]
   (prepare-query conn query-string {}))
  ([conn query-string bindings]
   (let [prepared (repo/prepare-query conn query-string)]
     (doseq [[k value] bindings]
       (.setBinding prepared (name k) (rio/->backend-type value)))
     prepared)))

(defn- evaluate-with-bindings
  "Evaluates the given SPARQL string with the specified bindings map"
  [conn query-string bindings]
  (let [prepared (prepare-query conn query-string bindings)]
    (repo/evaluate prepared)))

(t/deftest query-bindings-test
  (tc/with-system
    [system "drafter/stasher-test/stasher-repo-cache-and-serve-test.edn"]
    (let [repo (:drafter.stasher/repo system)
          ;;bindings that should match in the data
          subject-uri (URI. "http://statistics.gov.scot/data/home-care-clients")
          matching-object "Home Care Clients"
          positive-bindings {:s subject-uri
                             :p rdfs:label}
          ;; bindings that should not match in the data
          negative-bindings {:s subject-uri
                             :o "Missing"}]
      (with-open [conn (repo/->connection repo)]
        (t/testing "boolean query"
          (let [q "ASK WHERE { ?s ?p ?o }"]
            (t/is (= true (evaluate-with-bindings conn q positive-bindings)) "Failed to match")
            (t/is (= false (evaluate-with-bindings conn q negative-bindings)) "Matched unexpectedly")))

        (t/testing "select query"
          (let [q "SELECT * WHERE { ?s ?p ?o }"]
            (t/testing "pull results"
              (t/testing "matching"
                (let [results (evaluate-with-bindings conn q positive-bindings)]
                  (t/is (= [{:o matching-object}] results) "Unexpected results")))
              (t/testing "non-matching"
                (t/is (nil? (seq (evaluate-with-bindings conn q negative-bindings))))))

            (t/testing "push results"
              (t/testing "matching"
                (let [[events handler] (recording-tuple-handler)
                      prepared (prepare-query conn q positive-bindings)]
                  (.evaluate prepared handler)
                  (let [expected [{:o matching-object}]
                        {:keys [data ended]} @events]
                    (t/is ended "Expected results to be consumed")
                    (t/is (= expected data) "Unexepcted bindings"))))
              (t/testing "non-matching"
                (let [[events handler] (recording-tuple-handler)
                      prepared (prepare-query conn q negative-bindings)]
                  (.evaluate prepared handler)
                  (let [{:keys [data ended]} @events]
                    (t/is ended "Expected results to be consumed")
                    (t/is (nil? (seq data)) "Unexpected matches")))))))

        (t/testing "construct query"
          (let [q "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }"]
            (t/testing "pull results"
              (t/testing "matching"
                (let [expected [(pr/->Triple subject-uri rdfs:label matching-object)]
                      results (evaluate-with-bindings conn q positive-bindings)]
                  (t/is (= expected results) "Unexpected results")))

              (t/testing "non-matching"
                (t/is (nil? (seq (evaluate-with-bindings conn q negative-bindings))))))

            (t/testing "push results"
              (t/testing "matching"
                (let [[events handler] (recording-rdf-handler)
                      prepared (prepare-query conn q positive-bindings)]
                  (.evaluate prepared handler)
                  (let [expected [(pr/->Triple subject-uri rdfs:label matching-object)]
                        {:keys [data ended]} @events]
                    (t/is ended "Expected results to be consumed")
                    (t/is (= expected data) "Unexpected results"))))

              (t/testing "non-matching"
                (let [[events handler] (recording-rdf-handler)
                      prepared (prepare-query conn q negative-bindings)]
                  (.evaluate prepared handler)
                  (let [{:keys [ended data]} @events]
                    (t/is ended "Expected results to be consumed")
                    (t/is (nil? (seq data)) "Unexpected matches")))))))))))

  (defn- get-open-file-count
    "Tries to get the number of file handles opened by this process"
    []
    (let [os-bean (java.lang.management.ManagementFactory/getOperatingSystemMXBean)]
      (if (instance? com.sun.management.UnixOperatingSystemMXBean os-bean)
        (.getOpenFileDescriptorCount os-bean)
        (throw (RuntimeException. "Unsupported OS - expected Unix derivative")))))

(defmacro with-file-handle-check [& body]
  `(let [initial-open-file-count# (get-open-file-count)]
     ;; try and trigger a GC incase a previous run has leaked file
     ;; counts that might get gc'd during this run.
     ;;
     ;; NOTE we can't 100% rely on this, but if the GC occurs and the
     ;; finalizers close any open files then it will make the numbers
     ;; more useful if there's been a previous error.
     ;;
     ;; It may also make the test less brittle to false positives (it
     ;; certainly shouldn't make it worse) by reducing the chance of
     ;; GC's happening during the test run.
     (.gc (Runtime/getRuntime))
     ~@body
     (t/is (>= initial-open-file-count# (get-open-file-count))
           "One or more file handles appear to have been left open.")))


(deftest-system closes-stasher-files-test
  [{:keys [drafter.stasher/repo
           drafter.stasher/filecache]} "drafter/stasher-test/drafter-state-1.edn"]

  (t/testing "Don't leak file handles by query type"
    ;; Here we perform each query twice to ensure we exercise both the
    ;; first cache miss and second cache hit paths for each query
    ;; type.

    (t/testing "SELECT"
      (let [select-query (fn [t] (with-open [conn (repo/->connection repo)]
                                  (let [qstr "select * where { ?s ?p ?o } limit 1"]
                                    (if (= :async t)
                                      (let [[events handler] (recording-tuple-handler)
                                            prepped-q (prepare-query conn qstr)]
                                        (.evaluate prepped-q handler)
                                        nil)
                                      (do (into [] (evaluate-with-bindings conn qstr {}))
                                          nil)))))]
        (t/testing "async"
          (t/testing "cache miss"
            (with-file-handle-check
              (select-query :async)))

          (t/testing "cache hit"
            (with-file-handle-check
              (select-query :async))))

        (t/testing "sync"
          (t/testing "cache miss"
            (with-file-handle-check
              (select-query :sync)))

          (t/testing "cache hit"
            (with-file-handle-check
              (select-query :sync))))))

    (t/testing "CONSTRUCT"
      (let [construct-query (fn [t] (with-open [conn (repo/->connection repo)]
                                     (let [qstr "construct where { ?s ?p ?o }"]
                                       (if (= :async t)
                                         (let [[events handler] (recording-rdf-handler)
                                               prepped-q (prepare-query conn qstr)]
                                           (.evaluate prepped-q handler))
                                         (into [] (evaluate-with-bindings conn qstr {}))))))]
        (t/testing "async"
          (t/testing "cache miss"
            (with-file-handle-check
              (construct-query :async)))

          (t/testing "cache hit"
            (with-file-handle-check
              (construct-query :async))))

        (t/testing "sync"
          (t/testing "cache miss"
            (with-file-handle-check
              (construct-query :sync)))

          (t/testing "cache hit"
            (with-file-handle-check
              (construct-query :sync))))))


    ;; commented out because it fails on travis, works locally with
    ;; more resources
    #_(t/testing "CONSTRUCT with timeout/error"
      (let [construct-query (fn [t] (with-open [conn (repo/->connection repo)]
                                     (let [qstr "construct where { ?s ?p ?o .  ?s1 ?p1 ?o1 .  ?s2 ?p2 ?o2 .  ?s3 ?p3 ?o3 }"]
                                       (try
                                         (if (= :async t)
                                           (let [[events handler] (recording-rdf-handler)
                                                 prepped-q (prepare-query conn qstr)]
                                             (.evaluate prepped-q handler))
                                           (into [] (evaluate-with-bindings conn qstr {})))
                                         (catch org.eclipse.rdf4j.repository.RepositoryException ex
                                           ;; expected exception
                                           nil)
                                         (catch clojure.lang.ExceptionInfo exi
                                           ;; also raised by sync path
                                           nil)))))]
        (t/testing "async"
          (t/testing "cache miss"
            (with-file-handle-check
              (construct-query :async)))

          (t/testing "cache hit"
            (with-file-handle-check
              (construct-query :async))))

        (t/testing "sync"
          (t/testing "cache miss"
            (with-file-handle-check
              (construct-query :sync)))

          (t/testing "cache hit"
            (with-file-handle-check
              (construct-query :sync))))))

    (t/testing "ASK"
      (t/testing "sync (no async variant for this query-type)"
        (let [ask-query (fn [] (with-open [conn (repo/->connection repo)]
                                (let [prepped-q (prepare-query conn "ask where { ?s ?p ?o }")]
                                  (.evaluate prepped-q))))]
          (with-file-handle-check
            (dotimes [_n 2]
              (ask-query))))))))



(comment

  (let [at (atom [])
        listener (reify RepositoryConnectionListener ;; todo can extract for in memory change detection
                   (add [this conn sub pred obj graphs]
                     #_(println "adding")
                     (swap! at conj [:add sub pred obj graphs]))
                   (begin [this conn]
                     #_(println "begin")
                     (swap! at conj [:begin]))
                   (close [this conn]
                     #_(println "close")
                     (swap! at conj [:close]))
                   (commit [this conn]
                     #_(println "commit")
                     (swap! at conj [:commit]))
                   (execute [this conn ql updt-str base-uri operation]
                     #_(println "execute")
                     (swap! at conj [:execute ql updt-str base-uri operation])))

        repo (doto (stasher-repo "http://localhost:5820/drafter-test-db/query" "http://localhost:5820/drafter-test-db/update")
               (.addRepositoryConnectionListener listener))]

    (with-open [conn (repo/->connection repo)]
      (pr/add conn [(pr/->Quad (URI. "http://foo") (URI. "http://foo") (URI. "http://foo") (URI. "http://foo"))])
      (println (doall (repo/query conn "construct { ?s ?p ?o } where { ?s ?p ?o } limit 10")))

      (pr/update! conn "drop all"))

    @at)

  (import '[java.io.File
            java.net.URI]
          '[java.lang.management ManagementFactory]
          '[com.sun.management UnixOperatingSystemMXBean]
          '[java.lang.management ManagementFactory])

  (get-open-file-count)
  )
