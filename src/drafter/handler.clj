(ns drafter.handler
  (:require [compojure.core :refer [defroutes routes]]
            [drafter.routes.pages :refer [pages-routes]]
            [drafter.routes.sparql :refer [live-sparql-routes draft-sparql-routes
                                           state-sparql-routes]]
            [drafter.routes.sparql-update :refer [update-endpoint-route]]
            [drafter.routes.drafts-api :refer [draft-api-routes graph-management-routes]]
            [drafter.routes.queue-api :refer [queue-api-routes]]
            [drafter.middleware :as middleware]
            [drafter.rdf.sparql-rewriting :refer [function-registry register-function pmdfunctions]]
            [drafter.routes.sparql-update :refer [update-endpoint-route state-update-endpoint-route live-update-endpoint-route]]
            [noir.util.middleware :refer [app-handler]]
            [compojure.route :as route]
            [taoensso.timbre :as timbre]
            [taoensso.timbre.appenders.rotor :as rotor]
            [selmer.parser :as parser]
            [drafter.rdf.draft-management :refer [graph-map lookup-live-graph-uri drafter-state-graph]]
            [drafter.rdf.queue :as q]
            [grafter.rdf.sesame :as sesame]
            [compojure.handler :only [api]]
            [environ.core :refer [env]]))

(def repo-path "MyDatabases/repositories/db")

;; Set these values later when we start the server
(def repo)
(def app)

(def worker)
(def queue (q/make-queue 10))

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

(defn initialise-repo! []
  (set-var-root! #'repo (let [repo (sesame/repo (sesame/native-store repo-path))]
                          (timbre/info "Initialised repo" repo-path)
                          repo))

  (register-sparql-extension-functions))

(defroutes app-routes
  (route/resources "/")
  (route/not-found "Not Found"))

(defn initialise-app! [repo queue]
  (set-var-root! #'app (app-handler
                        ;; add your application routes here
                        [pages-routes
                         (draft-api-routes "/draft" repo queue)
                         (graph-management-routes "/graph" repo queue)
                         (queue-api-routes "/queue" queue)
                         (live-sparql-routes "/sparql/live" repo)
                         (live-update-endpoint-route "/sparql/live/update" repo)
                         (draft-sparql-routes "/sparql/draft" repo)
                         (update-endpoint-route "/sparql/draft/update" repo)
                         (state-sparql-routes "/sparql/state" repo)
                         (state-update-endpoint-route "/sparql/state/update" repo)
                         app-routes]
                        ;; add custom middleware here
                        :middleware [middleware/template-error-page
                                     middleware/log-request]
                        ;; add access rules here
                        :access-rules []
                        ;; serialize/deserialize the following data formats
                        ;; available formats:
                        ;; :json :json-kw :yaml :yaml-kw :edn :yaml-in-html
                        :formats [:json-kw :edn])))

(defn attach-worker!
  "Attach the process-queue worker to process the jobs queue for
  requests to append/replace graphs with RDF files."
  [queue]
  (set-var-root! #'worker
                 (q/process-queue queue
                                  (fn [ex]
                                    (taoensso.timbre/error
                                     (str "Queue Worker Error.  Repo id is: " (System/identityHashCode repo) " Queue contains: " queue) ex))))

  (timbre/info "Attached import worker to job queue"))

(defn initialise-services! []
  (initialise-repo!)
  (attach-worker! queue)
  (initialise-app! repo queue))

(defn init
  "init will be called once when app is deployed as a servlet on an
  app server such as Tomcat put any initialization code here"
  []
  (timbre/set-config!
    [:appenders :rotor]
    {:min-level :fatal
     :enabled? true
     :async? false ; should be always false for rotor
     :max-message-per-msecs nil
     :fn rotor/appender-fn})

  (timbre/set-config!
    [:shared-appender-config :rotor]
    {:path "drafter.log" :max-size (* 512 1024) :backlog 10})

  (when (env :dev)
    (parser/cache-off!)
    (timbre/merge-config! {:appenders
                         ;; disable colouring of output in dev env as
                         ;; it causes stacktraces to become invisible
                         ;; in Emacs.
                         {:standard-out { :fmt-output-opts {:nofonts? true}}}}))

  (initialise-services!)

  (timbre/info "drafter started successfully"))

(defn destroy
  "destroy will be called when your application
   shuts down, put any clean up code here"
  []
  (timbre/info "drafter is shutting down.  Please wait (this can take a minute)...")
  (sesame/shutdown repo)
  (future-cancel worker)
  (timbre/info "drafter has shut down."))
