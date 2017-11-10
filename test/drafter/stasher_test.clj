(ns drafter.stasher-test
  (:require [drafter.stasher :as sut]
            [grafter.rdf4j.io :as gio]
            [clojure.test :as t]
            [drafter.test-common :as tc]
            [clojure.java.io :as io]
            [grafter.rdf :as rdf]
            [drafter.stasher.filecache :as fc]
            [clojure.core.cache :as cache]
            [grafter.rdf4j.repository :as repo])
  (:import [java.net URI]))

(t/use-fixtures :each (tc/wrap-system-setup (io/resource "drafter/stasher-test/system.edn") [:drafter.stasher/repo]))

(def test-triples [(rdf/->Quad (URI. "http://foo") (URI."http://is/a") (URI."http://is/triple") nil)])

(defn- add-rdf-file-to-cache! [cache cache-key]
  (let [tf (java.io.File/createTempFile "drafter.stasher.filecache-test" ".tmp")]

    (rdf/add (gio/rdf-writer tf :format (fc/backend-rdf-format cache))
             test-triples)
    
    ;; move file in to cache
    (cache/miss cache cache-key tf)))

(def basic-construct-query "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o . }")

(t/deftest stasher-repo-test-return-cache-hit
  (let [repo (:drafter.stasher/repo tc/*test-system*)
        cache (:drafter.stasher/filecache tc/*test-system*)]

    (t/testing "Querying a cached value returns the cached RDF"

      (add-rdf-file-to-cache! cache basic-construct-query) ;; sneak a file in to the cache via the backdoor

      (t/is (= test-triples
               (repo/query (repo/->connection repo) basic-construct-query))))))

(def fixture-file "drafter/stasher-test/fixture-data.ttl")

(defn load-fixture! [repo fixture-resource]
  (with-open [conn (repo/->connection repo)]
    (rdf/add conn (rdf/statements (io/resource fixture-resource) :format :ttl))))

(t/deftest stasher-repo-test-cache-and-serve
  (let [repo (:drafter.stasher/repo tc/*test-system*) 
        cache (:drafter.stasher/filecache tc/*test-system*)]

    (load-fixture! repo fixture-data)
    
    (t/testing "A cache miss returns the results and stores them in the cache"
      (with-open [conn (repo/->connection repo)]
        (let [uncached-results (repo/query conn basic-construct-query)
              cached-results (repo/query conn basic-construct-query)
              fixture-data (rdf/statements (io/resource fixture-data) :format :ttl)]

          (t/testing "The cached, uncached and fixture data are all the same"
            (t/is (= (set uncached-results)
                     (set cached-results)
                     (set fixture-data))))

          (t/testing "Results for query are stored on disk"
            ;; evidence that we didn't just run another uncached query
            (let [cached-file (fc/cache-key->cache-path cache basic-construct-query)
                  cached-file-statements (-> cached-file
                                        io/input-stream
                                        (rdf/statements :format (fc/backend-rdf-format cache)))]

              (t/testing "Prove the side-effect of creating the file in the cache happened"
                (t/is (.exists cached-file)))

              (t/testing "Prove the file that was written to the cache is the same as the fixture data that went in"
                (t/is (= (set cached-file-statements)
                         (set fixture-data)))))))))))



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
