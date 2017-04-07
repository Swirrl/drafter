(ns drafter.env
  "Production environment code for Drafter")

(def environment
  "The environment that was packaged with this Drafter build via a
  leiningen classifier."
  :prod)

(def env-specific-middlewares
  "Environment specific middlewares.  These ones are only included in
  the prod env."
  [])

(defn env-specific-routes
  [db-backend]
  [])
