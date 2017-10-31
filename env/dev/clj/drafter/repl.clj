(ns drafter.repl
  (:require [drafter.main :refer [start-system! stop-system!]]
            [eftest.runner :as eftest]
            [clojure.java.io :as io]))

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
