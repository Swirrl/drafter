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

(t/use-fixtures :once (tc/wrap-system-setup (io/resource "drafter/stasher-test.edn") [:drafter.stasher/repo]))

(def test-triples [(rdf/->Quad (URI. "http://foo") (URI."http://is/a") (URI."http://is/triple") nil)])

(defn- add-rdf-file-to-cache! [cache cache-key]
  (let [tf (java.io.File/createTempFile "drafter.stasher.filecache-test" ".tmp")]

    (rdf/add (gio/rdf-writer tf :format (fc/backend-rdf-format cache))
             test-triples)
    
    ;; move file in to cache
    (cache/miss cache cache-key tf)))

(t/deftest stasher-repo-test
  (let [repo (:drafter.stasher/repo tc/*test-system*)
        cache (:drafter.stasher/filecache tc/*test-system*)]

    (t/testing "Returns cached RDF"
      (let [stub-sparql-query "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o . }"]

        (add-rdf-file-to-cache! cache stub-sparql-query)

        (t/is (= test-triples
                 (repo/query (repo/->connection repo) stub-sparql-query)))))))

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
