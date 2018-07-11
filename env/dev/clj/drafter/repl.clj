(ns drafter.repl
  (:require [drafter.main :refer [start-system! stop-system! system]]
            [eftest.runner :as eftest]
            [clojure.java.io :as io]
            [grafter.rdf4j.repository :as repo]
            [grafter.rdf.protocols :as pr]))

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
    (println "(run-tests)"))

(comment

  (do
    (require '[grafter.rdf.protocols :as pr])
    (require '[grafter.rdf4j.repository :as repo])

    (def repo (:drafter.backend/rdf4j-repo system))
    (def conn (repo/->connection repo))
    
    

    (repo/query conn "PREFIX : <http://example> \n select * where { ?s ?p ?o } limit 10"))

  
  )
