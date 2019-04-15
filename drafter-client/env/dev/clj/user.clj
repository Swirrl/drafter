(ns user
  (:require [integrant.core :as ig]
            [integrant.repl :refer [clear go halt prep init reset reset-all]]
            [integrant.repl.state :refer [config system]]
            [drafter-client.client :as dc]))

(integrant.repl/set-prep!
 (constantly
  (ig/read-string (slurp (clojure.java.io/resource "dev-system.edn")))))


(comment

  config
  system

  (prep)

  (go)

  (halt)

  (reset)

  )
