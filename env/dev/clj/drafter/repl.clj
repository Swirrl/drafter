(ns drafter.repl
  (:require [clojure.pprint :as pprint]
            [clojure.spec.test.alpha :as st]
            [clojure.spec.alpha :as s]
            [drafter.check-specs :refer [check-specs]]
            [drafter.main :as main :refer [system stop-system!]]
            [eftest.runner :as eftest]
            [grafter-2.rdf4j.repository :as repo]
            [integrant.core :as ig]
            [clojure.java.io :as io]
            [meta-merge.core :as mm]))

(def profiles [(io/resource "drafter-base-config.edn") (io/resource "drafter-dev-config.edn") (io/resource "drafter-local-config.edn")])

(defn stub-fdefs [set-of-syms]
  (st/instrument set-of-syms {:stub set-of-syms}))

(defn start-system!
  ([] (start-system! {:instrument? true}))
  ([{:keys [instrument?] :as opts}]

   (main/start-system! (apply mm/meta-merge (->> profiles
                                                 (remove nil?)
                                                 (map main/read-system))))
   (when instrument?
     (st/instrument))))

(defn run-tests []
  (eftest/run-tests (eftest/find-tests "test") {:multithread? false}))

(do (println)
    (println "   ___           _____         ")
    (println "  / _ \\_______ _/ _/ /____ ____")
    (println " / // / __/ _ `/ _/ __/ -_) __/")
    (println "/____/_/  \\_,_/_/ \\__/\\__/_/   ")
    (println)
    (println "Welcome to the Drafter REPL!")
    (println)
    (println)
    (println "REPL Commands: ")
    (println)
    (println "(start-system!)")
    (println "(stop-system!)")
    (println "(run-tests)")
    (println)
    (println "(check-specs 100)"))

(comment

  (do
    (require '[grafter.core :as pr])
    (require '[grafter-2.rdf4j.repository :as repo])

    (def repo (:drafter.backend/rdf4j-repo system))
    (def conn (repo/->connection repo))



    (repo/query conn "PREFIX : <http://example> \n select * where { ?s ?p ?o } limit 10"))


  )
