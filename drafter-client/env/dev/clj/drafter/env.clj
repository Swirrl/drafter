(ns drafter.env
  "Development env code for Drafter")

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
  [])
