;; TODO replace with a dev env repl namespace with useful utilities.  Original has moved to drafter.server
(ns drafter.repl)

(defn dev []
  (require 'drafter.main)
  (in-ns 'drafter.main))


(do (println)
    (println "   ___           _____         ")
    (println "  / _ \\_______ _/ _/ /____ ____")
    (println " / // / __/ _ `/ _/ __/ -_) __/")
    (println "/____/_/  \\_,_/_/ \\__/\\__/_/   ")
    (println)
    (println "Welcome to the Drafter REPL!")
    (println)
    (println)
    (println "Run: (dev)"))




