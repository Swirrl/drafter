;; TODO replace with a dev env repl namespace with useful utilities.  Original has moved to drafter.server
(ns drafter.repl)

(defn dev []
  (require 'drafter.main)
  (in-ns 'drafter.main))


(println "Run (dev)")



