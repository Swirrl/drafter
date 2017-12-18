(ns drafter.stasher-test
  (:require [drafter.stasher :as sut]
            [grafter.rdf4j.io :as gio]
            [clojure.test :as t]
            [drafter.test-common :as tc :refer [with-system deftest-system]]
            [clojure.java.io :as io]
            [grafter.rdf :as rdf]
            [drafter.stasher.filecache :as fc]
            [clojure.core.cache :as cache]
            [grafter.rdf4j.repository :as repo]
            [grafter.rdf.protocols :as pr]
            [grafter.url :as url]
            [integrant.core :as ig])
  (:import [java.net URI]
           org.eclipse.rdf4j.rio.RDFHandler
           (org.eclipse.rdf4j.model.impl URIImpl)
           (org.eclipse.rdf4j.query.impl SimpleDataset)))

(def test-triples [(rdf/->Quad (URI. "http://foo") (URI."http://is/a") (URI."http://is/triple") nil)])

(defn- add-rdf-file-to-cache!
  "Force the creation of an entry in the cache via the backdoor "
  [cache cache-key]
  (let [tf (java.io.File/createTempFile "drafter.stasher.filecache-test" ".tmp")]

    (rdf/add (gio/rdf-writer tf :format (fc/backend-rdf-format cache))
             test-triples)
    
    ;; move file in to cache
    (cache/miss cache cache-key tf)))

(def basic-construct-query "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o . }")

(defmethod ig/init-key ::stub-cache-key-generator [_ opts]
  (fn [cache query-str dataset {repo :raw-repo :as context}]
    ;; Just use query-str as cache key for stasher-repo-test-return-cache-hit
    query-str))

(deftest-system stasher-repo-return-cache-hit-test
  [{:keys [drafter.stasher/repo drafter.stasher/filecache]}  "drafter/stasher-test/stasher-repo-return-cache-hit-test.edn"]
  (t/testing "Querying a cached value returns the cached RDF"
    ;; sneak a file in to the cache via the backdoor - mutable file system muuhahahaha!!
    (let [cache-key basic-construct-query]

      ;; TODO generalise this to work with SELECT/BOOLEANs
      (add-rdf-file-to-cache! filecache basic-construct-query)
      
      (t/is (= test-triples
               (repo/query (repo/->connection repo) basic-construct-query))))))

(defn assert-cached-results 
  [cache raw-repo query dataset expected-data]
  ;; evidence that we didn't just run another uncached query
  (let [cache-key (sut/generate-drafter-cache-key :graph cache query dataset {:raw-repo raw-repo})
        cached-file (fc/cache-key->cache-path cache cache-key)
        cached-file-statements (-> cached-file
                                   io/input-stream
                                   (rdf/statements :format (fc/backend-rdf-format cache)))]

    (t/testing "Prove the side-effect of creating the file in the cache happened"
      (t/is (.exists cached-file)))

    (t/testing "Prove the file that was written to the cache is the same as the fixture data that went in"
      (t/is (= (set cached-file-statements)
               (set expected-data))))))

(defn box-result [res]
  (if (boolean? res)
    [res]
    res))

(defn assert-caches-query [repo cache query-str expected-data]
  (t/testing "Cached & uncached queries return expected data and expected data is stored in the cache"
    (with-open [conn (repo/->connection repo)]
      (let [uncached-results (box-result (repo/query conn query-str)) ;; first query should be uncached
            cached-results (box-result (repo/query conn query-str))]
        
        (t/testing "The cached, uncached and fixture data are all the same"
          (t/is (= (set uncached-results)
                   (set cached-results)
                   (set (box-result expected-data)))))

        (t/testing "Results for query are stored on disk"
          ;; Check that the expected fixture data was stored in the cache on disk
          (assert-cached-results cache repo query-str nil expected-data))))))

(deftest-system stasher-queries-pull-test
  [{caching-repo :drafter.stasher/repo
    cache :drafter.stasher/filecache
    uncached-repo :drafter.backend/rdf4j-repo
    {:keys [fixtures]} :drafter.fixture-data/loader} "drafter/stasher-test/stasher-repo-cache-and-serve-test.edn"]

  (t/testing "Stashing of all query types"
    (tc/TODO (t/testing "ASK"
               (assert-caches-query caching-repo cache "ASK WHERE { ?s ?p ?o }" #{true})
               (assert-caches-query caching-repo cache "ASK WHERE { <http://not> <http://in> <http://db> }" #{false})))
    
    (t/testing "SELECT"
      (let [select-query "SELECT * WHERE { ?s ?p ?o } LIMIT 2"
            expected-data (doall (repo/query (repo/->connection uncached-repo) select-query))]
        (assert-caches-query caching-repo cache select-query expected-data)))

    (let [graph-data (rdf/statements (first fixtures) :format :ttl)]
      (t/testing "CONSTRUCT"
        (assert-caches-query caching-repo cache basic-construct-query graph-data))

      (tc/TODO
       (t/testing "DESCRIBE" 
         (assert-caches-query caching-repo cache "DESCRIBE <http://statistics.gov.scot/data/home-care-clients>" graph-data))))))

(defn recording-rdf-handler
  "Convenience function that returns a 2-tuple rdf-handler that will
  record events inside an atom.

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
                           (swap! recorded-events update :statements conj (gio/backend-quad->grafter-quad statement)))
                         (handleComment [this comment]
                           (swap! recorded-events update :comments conj comment))
                         (handleNamespace [this prefix uri]
                           (swap! recorded-events update :namespaces assoc prefix uri)))]

    [recorded-events rdf-handler]))

(deftest-system stasher-repo-test-cache-and-serve-push
  [{repo :drafter.stasher/repo
    cache :drafter.stasher/filecache
    raw-repo :drafter.backend/rdf4j-repo
    {:keys [fixtures]} :drafter.fixture-data/loader} "drafter/stasher-test/stasher-repo-cache-and-serve-test.edn"]
  (t/testing "Push interface (RDFHandler) to query results"
    (t/testing "A cache miss returns the results and stores them in the cache"
      (with-open [conn (repo/->connection repo)]
        (let [preped-query (.prepareGraphQuery conn basic-construct-query)
              [uncached-recorded-events uncached-rdf-event-handler] (recording-rdf-handler)
              [cached-recorded-events cached-rdf-event-handler] (recording-rdf-handler)]

          (.evaluate preped-query uncached-rdf-event-handler) ;; run uncached query 
          (.evaluate preped-query cached-rdf-event-handler)   ;; run again (the cached query)

          (t/is (= true
                   (:started @uncached-recorded-events)
                   (:started @cached-recorded-events))
                "startRDF called")

          (t/is (= true
                   (:ended @uncached-recorded-events)
                   (:ended @cached-recorded-events))
                "endRDF called")

          (t/testing "The cached, uncached queries return the same data"
            (t/is (= (set (:statements @uncached-recorded-events))
                     (set (:statements @cached-recorded-events)))))
          
          (t/testing "Results for query are stored on disk"
            ;; evidence that we didn't just run another uncached query

            (let [cache-key (sut/generate-drafter-cache-key :graph cache basic-construct-query nil {:raw-repo raw-repo})
                  cached-file (fc/cache-key->cache-path cache cache-key)
                  cached-file-statements (-> cached-file
                                             io/input-stream
                                             (rdf/statements :format (fc/backend-rdf-format cache)))]

              (t/testing "Prove the side-effect of creating the file in the cache happened"
                (t/is (.exists cached-file)))
              
              (t/testing "The cached, uncached and fixture data are all the same"
                (t/is (= (set (:statements @uncached-recorded-events))
                         (set (:statements @cached-recorded-events))
                         (set cached-file-statements))))
              
              (t/testing "Prove the file that was written to the cache is the same as the fixture data that went in"
                (t/is (= (set (rdf/statements (first fixtures) :format :ttl))
                         (set cached-file-statements)))))))))))

(deftest-system stasher-queries-push-test
  [{repo :drafter.stasher/repo
    cache :drafter.stasher/filecache
    raw-repo :drafter.backend/rdf4j-repo
    {:keys [fixtures]} :drafter.fixture-data/loader} "drafter/stasher-test/stasher-repo-cache-and-serve-test.edn"]

  (t/testing "CONSTRUCT"
    (t/testing "Push interface (RDFHandler) to query results"
      (t/testing "A cache miss returns the results and stores them in the cache"
        (with-open [conn (repo/->connection repo)]
          (let [preped-query (.prepareGraphQuery conn basic-construct-query)
                [uncached-recorded-events uncached-rdf-event-handler] (recording-rdf-handler)
                [cached-recorded-events cached-rdf-event-handler] (recording-rdf-handler)]

            (.evaluate preped-query uncached-rdf-event-handler) ;; run uncached query 
            (.evaluate preped-query cached-rdf-event-handler)   ;; run again (the cached query)

            (t/is (= true
                     (:started @uncached-recorded-events)
                     (:started @cached-recorded-events))
                  "startRDF called")

            (t/is (= true
                     (:ended @uncached-recorded-events)
                     (:ended @cached-recorded-events))
                  "endRDF called")

            (t/testing "The cached, uncached queries return the same data"
              (t/is (= (set (:statements @uncached-recorded-events))
                       (set (:statements @cached-recorded-events)))))
            
            (t/testing "Results for query are stored on disk"
              ;; evidence that we didn't just run another uncached query

              (let [cache-key (sut/generate-drafter-cache-key :graph cache basic-construct-query nil {:raw-repo raw-repo})
                    cached-file (fc/cache-key->cache-path cache cache-key)
                    cached-file-statements (-> cached-file
                                               io/input-stream
                                               (rdf/statements :format (fc/backend-rdf-format cache)))]

                (t/testing "Prove the side-effect of creating the file in the cache happened"
                  (t/is (.exists cached-file)))
                
                (t/testing "The cached, uncached and fixture data are all the same"
                  (t/is (= (set (:statements @uncached-recorded-events))
                           (set (:statements @cached-recorded-events))
                           (set cached-file-statements))))
                
                (t/testing "Prove the file that was written to the cache is the same as the fixture data that went in"
                  (t/is (= (set (rdf/statements (first fixtures) :format :ttl))
                           (set cached-file-statements))))))))))))

(t/deftest dataset->edn-test
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

    (t/testing "dataset->edn prints same value irrespective of assembly order"
      (t/is (= (pr-str (sut/dataset->edn ds-1))
               (pr-str (sut/dataset->edn ds-2)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Drafter State Graph Stashing
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Definitions corresponding to idenfitiers/states in drafter-state-1.trig
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def live-graph-1 (URIImpl. "http://live-and-ds1-and-ds2"))
(def live-graph-only (URIImpl. "http://live-only"))

(def liveset-most-recently-modified {:livemod #inst "2017-02-02T02:02:02.000-00:00"})

(def ds-1-dg-1 (URIImpl. "http://publishmydata.com/graphs/drafter/draft/ds-1-dg-1"))
(def ds-1-dg-2 (URIImpl. "http://publishmydata.com/graphs/drafter/draft/ds-1-dg-2"))

(def ds-1 "draftset-1 is made of dg-1 dg-2" #{ds-1-dg-1 ds-1-dg-2})

(def ds-1-most-recently-modified "modified time of dg-2 the most recently modified graph in ds1"
  {:draftmod #inst "2017-04-04T04:04:04.000-00:00"})


(def ds-2-dg-1 (URIImpl. "http://publishmydata.com/graphs/drafter/draft/ds-2-dg-1"))

(def ds-2 #{ds-2-dg-1})

(def ds-2-most-recently-modified "modified time of dg-2 the most recently modified graph in ds1"
  {:draftmod #inst "2017-05-05T05:05:05.000-00:00"})

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

(deftest-system fetch-modified-state-test
  ;; Here we're just testing the underlying modified time queries, so we do
  ;; so on a standard RDF4j repo, not a stasher/caching one.
  [{repo :drafter.backend/rdf4j-repo} "drafter/stasher-test/drafter-state-1.edn"]  

  (t/testing "Fetching draftset modified times"
    (t/is (= ds-1-most-recently-modified
             (sut/fetch-modified-state repo (edn->dataset {:named-graphs ds-1 :default-graphs ds-1}))))

    (t/is (= ds-2-most-recently-modified
             (sut/fetch-modified-state repo (edn->dataset {:named-graphs ds-2 :default-graphs ds-2})))))

  (t/testing "Fetching live graph modified times"
    (t/is (= liveset-most-recently-modified
             (sut/fetch-modified-state repo (edn->dataset {:named-graphs [live-graph-1 live-graph-only] :default-graphs [live-graph-1 live-graph-only]})))))

    (t/testing "Fetching draftsets with union-with-live set"
      ;; Union with live is at this low level equivalent to merging the
      ;; set of live graphs in to :named-graphs and :default-graphs.
      (let [ds-1-union-with-live (conj ds-1 live-graph-1 live-graph-only)
            dataset (edn->dataset {:named-graphs ds-1-union-with-live :default-graphs ds-1-union-with-live})]
        (t/is (= (merge liveset-most-recently-modified
                        ds-1-most-recently-modified)
                 (sut/fetch-modified-state repo dataset))))))

(deftest-system generate-drafter-cache-key-test
  [{:keys [drafter.stasher/repo
           drafter.stasher/filecache]} "drafter/stasher-test/drafter-state-1.edn"]

  (let [dataset (edn->dataset {:named-graphs [live-graph-1 live-graph-only]
                               :default-graphs [live-graph-1 live-graph-only]})
        result (sut/generate-drafter-cache-key :graph filecache basic-construct-query dataset {:raw-repo repo})]
    
    (let [{:keys [dataset query-str modified-times]} result]
      (t/is (= 
             {:default-graphs ["http://live-and-ds1-and-ds2" "http://live-only"],
              :named-graphs ["http://live-and-ds1-and-ds2" "http://live-only"]}
             dataset))
      (t/is (= basic-construct-query
               query-str))
      (t/is (= modified-times
               {:livemod #inst "2017-02-02T02:02:02.000-00:00"})))))


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
    (rdf/add conn [(pr/->Quad (URI. "http://foo") (URI. "http://foo") (URI. "http://foo") (URI. "http://foo"))])
    (println (doall (repo/query conn "construct { ?s ?p ?o } where { ?s ?p ?o } limit 10")))

    (pr/update! conn "drop all"))
  
  @at)

  )
