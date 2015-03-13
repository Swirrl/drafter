(ns drafter.handler
  (:require [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [compojure.core :refer [defroutes context]]
            [compojure.route :as route]
            [drafter.middleware :as middleware]
            [drafter.common.json-encoders :as enc]
            [drafter.rdf.draft-management :refer [lookup-live-graph-uri]]
            [drafter.rdf.sparql-rewriting :refer [function-registry
                                                  pmdfunctions
                                                  register-function]]
            [drafter.routes.drafts-api :refer [draft-api-routes
                                               graph-management-routes]]
            [drafter.routes.status :refer [status-routes]]
            [drafter.routes.dumps :refer [dumps-endpoint]]
            [drafter.routes.pages :refer [pages-routes]]
            [drafter.routes.sparql :refer [draft-sparql-routes
                                           live-sparql-routes
                                           raw-sparql-routes
                                           state-sparql-routes]]
            [drafter.routes.sparql-update :refer [draft-update-endpoint-route
                                                  live-update-endpoint-route
                                                  raw-update-endpoint-route
                                                  state-update-endpoint-route]]
            [drafter.write-scheduler :refer [start-writer!
                                             global-writes-lock
                                             finished-jobs
                                             stop-writer!]]
            [environ.core :refer [env]]
            [grafter.rdf.repository :as repo]
            [noir.util.middleware :refer [app-handler]]
            [ring.middleware.verbs :refer [wrap-verbs]]
            [selmer.parser :as parser])

  ;; Note that though the classes and requires below aren't used in this namespace
  ;; they are needed by the log-config file which is loaded from here.
  (:import [org.apache.log4j ConsoleAppender DailyRollingFileAppender EnhancedPatternLayout PatternLayout SimpleLayout]
           [org.apache.log4j.helpers DateLayout]
           [java.util UUID])

  (:require [clj-logging-config.log4j :refer [set-loggers!]]))

(defonce process-id (UUID/randomUUID))

(def default-repo-path "drafter-db")

;; Set these values later when we start the server
(def repo)
(def app)

(def worker)

(def ^{:doc "A future to control the single write thread that performs database writes."}
  writer-service)

(defmacro set-var-root! [var form]
  `(alter-var-root ~var (fn [& _#]
                         ~form)))

(defn register-sparql-extension-functions
  "Register custom drafter SPARQL extension functions."
  []

  ;; This function converts draft graphs into live graph URI's and is
  ;; necessary for drafters query/result rewriting to work.
  (register-function function-registry
                     (pmdfunctions "replace-live-graph")
                     (partial lookup-live-graph-uri repo)))

(defn initialise-repo! [repo-path indexes]
  (set-var-root! #'repo (let [repo (repo/repo (repo/native-store repo-path indexes))]
                          (log/info "Initialised repo" repo-path)
                          repo))

  (register-sparql-extension-functions))

(defroutes app-routes
  (route/resources "/")
  (route/not-found "Not Found"))

(defn initialise-app! [repo]
  (set-var-root! #'app (app-handler
                        ;; add your application routes here
                        [(pages-routes repo)
                         (draft-api-routes "/draft" repo process-id)
                         (graph-management-routes "/graph" repo process-id)

                         (live-sparql-routes "/sparql/live" repo)
                         (live-update-endpoint-route "/sparql/live/update" repo)
                         (dumps-endpoint "/data/live" live-sparql-routes repo)

                         (raw-sparql-routes "/sparql/raw" repo)
                         (raw-update-endpoint-route "/sparql/raw/update" repo)
                         (dumps-endpoint "/data/raw" raw-sparql-routes repo)

                         (draft-sparql-routes "/sparql/draft" repo)
                         (draft-update-endpoint-route "/sparql/draft/update" repo)
                         (dumps-endpoint "/data/draft" draft-sparql-routes repo)

                         (state-sparql-routes "/sparql/state" repo)
                         (state-update-endpoint-route "/sparql/state/update" repo)

                         (context "/status" []
                                  (status-routes global-writes-lock finished-jobs process-id))

                         app-routes]
                        ;; add custom middleware here
                        :middleware [wrap-verbs
                                     middleware/template-error-page
                                     middleware/log-request]
                        ;; add access rules here
                        :access-rules []
                        ;; serialize/deserialize the following data formats
                        ;; available formats:
                        ;; :json :json-kw :yaml :yaml-kw :edn :yaml-in-html
                        :formats [:json-kw :edn])))

(defn initialise-write-service! []
  (set-var-root! #'writer-service  (start-writer!)))

(defn initialise-services! [repo-path indexes]
  (enc/register-custom-encoders!)
  (initialise-repo! repo-path indexes)
  (initialise-write-service!)
  (initialise-app! repo))

(defn- load-logging-configuration [config-file]
  (-> config-file slurp read-string))

(defn configure-logging! [config-file]
  (binding [*ns* (find-ns 'drafter.handler)]
    (let [default-config (load-logging-configuration (io/resource "log-config.edn"))
          config-file (when (.exists config-file)
                        (load-logging-configuration config-file))]

      (let [chosen-config (or config-file default-config)]
        (eval chosen-config)
        (log/debug "Loaded logging config" chosen-config)))))

(defn init
  "init will be called once when app is deployed as a servlet on an
  app server such as Tomcat put any initialization code here"
  []

  (configure-logging! (io/file (get env :log-config-file "log-config.edn")))

  (when (env :dev)
    (parser/cache-off!))

  (initialise-services! (get env :drafter-repo-path default-repo-path)

                        ;; http://sw.deri.org/2005/02/dexa/yars.pdf - see table on p5 for full coverage of indexes.
                        ;; (but we have to specify 4 char strings, so in some cases last chars don't matter
                        (get env :drafter-indexes "spoc,pocs,ocsp,cspo,cpos,oscp"))

  (log/info "drafter started successfully"))

(defn destroy
  "destroy will be called when your application
   shuts down, put any clean up code here"
  []
  (log/info "drafter is shutting down.  Please wait (this can take a minute)...")
  (repo/shutdown repo)
  (future-cancel worker)
  (stop-writer! writer-service)
  (log/info "drafter has shut down."))
