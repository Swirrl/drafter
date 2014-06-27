(ns drafter.handler
  (:require [compojure.core :refer [defroutes]]
            [drafter.routes.home :refer [home-routes]]
            [drafter.routes.sparql :refer [live-sparql-routes drafts-sparql-routes]]
            [drafter.middleware :as middleware]
            [noir.util.middleware :refer [app-handler]]
            [compojure.route :as route]
            [taoensso.timbre :as timbre]
            [taoensso.timbre.appenders.rotor :as rotor]
            [selmer.parser :as parser]
            [grafter.rdf.sesame :as sesame]
            [environ.core :refer [env]]))

(def repo-path "MyDatabases/repositories/db")

;; Set these values later when we start the server
(def repo)
(def app)

(defmacro set-var-root! [var form]
  `(alter-var-root ~var (fn [& _#]
                         ~form)))

(defn initialise-repo! []
  (set-var-root! #'repo (let [repo (sesame/repo (sesame/native-store repo-path ))]
                          (timbre/info "Initialised repo in " repo-path)
                          repo)))

(defroutes app-routes
  (route/resources "/")
  (route/not-found "Not Found"))

(defn initialise-app! [repo]
  (set-var-root! #'app (app-handler
                        ;; add your application routes here
                        [home-routes (live-sparql-routes repo) (drafts-sparql-routes repo) app-routes]
                        ;; add custom middleware here
                        :middleware [middleware/template-error-page
                                     middleware/log-request]
                        ;; add access rules here
                        :access-rules []
                        ;; serialize/deserialize the following data formats
                        ;; available formats:
                        ;; :json :json-kw :yaml :yaml-kw :edn :yaml-in-html
                        :formats [:json-kw :edn])))

(defn init
  "init will be called once when
   app is deployed as a servlet on
   an app server such as Tomcat
   put any initialization code here"
  []
  (timbre/set-config!
    [:appenders :rotor]
    {:min-level :info
     :enabled? true
     :async? false ; should be always false for rotor
     :max-message-per-msecs nil
     :fn rotor/appender-fn})

  (timbre/set-config!
    [:shared-appender-config :rotor]
    {:path "drafter.log" :max-size (* 512 1024) :backlog 10})

  (if (env :dev) (parser/cache-off!))

  (initialise-repo!)
  (initialise-app! repo)

  (timbre/info "drafter started successfully"))

(defn destroy
  "destroy will be called when your application
   shuts down, put any clean up code here"
  []
  (timbre/info "drafter is shutting down...")
  (sesame/shutdown repo)
  (timbre/info "drafter has shut down."))
