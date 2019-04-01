(ns drafter.env
  "Development env code for Drafter"
  (:require [drafter.routes.dump :as dump]))

(def environment
  "The environment that was packaged with this Drafter build via a
  leiningen classifier."
  :dev)


(def env-specific-middlewares
  "Environment specific middlewares.  These ones are only included in
  the dev env."
  [])

(defn env-specific-routes
  "Environment specific ring routes."
  [db-repo]
  [(dump/build-dump-route db-repo)])
