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

#_(t/use-fixtures :each (tc/wrap-system-setup "drafter/stasher-test/system.edn" [:drafter.stasher/repo]))


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
      (add-rdf-file-to-cache! filecache basic-construct-query)
      
      (t/is (= test-triples
               (repo/query (repo/->connection repo) basic-construct-query))))))

(deftest-system stasher-repo-cache-and-serve-test
  [{repo :drafter.stasher/repo
    cache :drafter.stasher/filecache
    raw-repo :drafter.backend/rdf4j-repo
    {:keys [fixtures]} :drafter.fixture-data/loader} "drafter/stasher-test/stasher-repo-cache-and-serve-test.edn"]

  (t/testing "A cache miss returns the results and stores them in the cache"
    (with-open [conn (repo/->connection repo)]
      (let [uncached-results (repo/query conn basic-construct-query)
            cached-results (repo/query conn basic-construct-query)
            fixture-data (rdf/statements (first fixtures) :format :ttl)]

        (t/testing "The cached, uncached and fixture data are all the same"
          (t/is (= (set uncached-results)
                   (set cached-results)
                   (set fixture-data))))

        (t/testing "Results for query are stored on disk"
          ;; evidence that we didn't just run another uncached query
          (let [cache-key (sut/generate-drafter-cache-key cache basic-construct-query nil {:raw-repo raw-repo})
                cached-file (fc/cache-key->cache-path cache cache-key)
                cached-file-statements (-> cached-file
                                           io/input-stream
                                           (rdf/statements :format (fc/backend-rdf-format cache)))]

            (t/testing "Prove the side-effect of creating the file in the cache happened"
              (t/is (.exists cached-file)))

            (t/testing "Prove the file that was written to the cache is the same as the fixture data that went in"
              (t/is (= (set cached-file-statements)
                       (set fixture-data))))))))))

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

            (let [cached-file (fc/cache-key->cache-path cache basic-construct-query)
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
;; clojure definitions corresponding to idenfitiers/states in drafter-state-1.trig
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def live-graph-1 (URI. "http://live-and-ds1-and-ds2"))
(def live-graph-only (URI. "http://live-only"))

(def liveset #{live-graph-1 live-graph-only})

(def liveset-most-recently-modified {:livemod #inst "2017-02-02T02:02:02.000-00:00"})

(def ds-1-dg-1 (URI. "http://publishmydata.com/graphs/drafter/draft/ds-1-dg-1"))
(def ds-1-dg-2 (URI. "http://publishmydata.com/graphs/drafter/draft/ds-1-dg-2"))

(def ds-1 "draftset-1 is made of dg-1 dg-2" #{ds-1-dg-1 ds-1-dg-2})

(def ds-1-most-recently-modified "modified time of dg-2 the most recently modified graph in ds1"
  {:draftmod #inst "2017-04-04T04:04:04.000-00:00"})


(def ds-2-dg-1 (URI. "http://publishmydata.com/graphs/drafter/draft/ds-2-dg-1"))

(def ds-2 #{ds-2-dg-1})

(def ds-2-most-recently-modified "modified time of dg-2 the most recently modified graph in ds1"
  {:draftmod #inst "2017-05-05T05:05:05.000-00:00"})

(deftest-system fetch-modified-state-test
  [{:keys [drafter.stasher/repo]}  "drafter/stasher-test/drafter-state-1.edn"]  

  (t/testing "Fetching draftset modified times"
    (t/is (= ds-1-most-recently-modified
             (sut/fetch-modified-state repo {:named-graphs ds-1 :default-graphs ds-1})))

    (t/is (= ds-2-most-recently-modified
             (sut/fetch-modified-state repo {:named-graphs ds-2 :default-graphs ds-2}))))

  (t/testing "Fetching live graph modified times"
    (t/is (= liveset-most-recently-modified
             (sut/fetch-modified-state repo {:named-graphs liveset :default-graphs liveset}))))

    (t/testing "Fetching draftsets with union-with-live set"
    ;; Union with live is at this low level equivalent to merging the
    ;; set of live graphs in to :named-graphs and :default-graphs.
    (let [ds-1-union-with-live (conj ds-1 live-graph-1 live-graph-only)]
      (t/is (= (merge liveset-most-recently-modified
                      ds-1-most-recently-modified)
               (sut/fetch-modified-state repo {:named-graphs ds-1-union-with-live :default-graphs ds-1-union-with-live}))))))

(deftest-system build-drafter-cache-key-test
  [{:keys [drafter.stasher/repo]}  "drafter/stasher-test/drafter-state-1.edn"]
  ;; TODO
  (t/is (seq (rdf/statements (repo/->connection repo)))))


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
