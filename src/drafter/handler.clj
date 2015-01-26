(ns drafter.handler
  (:require [compojure.core :refer [defroutes routes]]
            [ring.middleware.verbs :refer [wrap-verbs]]
            [drafter.routes.pages :refer [pages-routes]]
            [drafter.routes.sparql :refer [live-sparql-routes draft-sparql-routes
                                           state-sparql-routes raw-sparql-routes]]
            [drafter.routes.dumps :refer [dumps-endpoint]]
            [drafter.routes.drafts-api :refer [draft-api-routes graph-management-routes]]
            [drafter.middleware :as middleware]
            [drafter.rdf.sparql-rewriting :refer [function-registry register-function pmdfunctions]]
            [drafter.routes.sparql-update :refer [draft-update-endpoint-route state-update-endpoint-route live-update-endpoint-route raw-update-endpoint-route]]
            [noir.util.middleware :refer [app-handler]]
            [compojure.route :as route]
            [selmer.parser :as parser]
            [drafter.rdf.draft-management :refer [graph-map lookup-live-graph-uri drafter-state-graph]]
            [grafter.rdf :refer [add statements subject predicate object]]
            [grafter.rdf.repository :as repo]
            [grafter.rdf.ontologies.rdf :refer :all]
            [compojure.handler :only [api]]
            [environ.core :refer [env]]
            [clojure.tools.logging :as log]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clj-logging-config.log4j :refer [set-loggers!]])
  (:import [org.apache.log4j ConsoleAppender DailyRollingFileAppender EnhancedPatternLayout PatternLayout SimpleLayout]
           [org.apache.log4j.helpers DateLayout]
           [org.openrdf.model.impl GraphImpl URIImpl BNodeImpl]
           [org.openrdf.repository.manager LocalRepositoryManager]
           [org.openrdf.repository.sail SailRepository]
           [org.openrdf.repository.config RepositoryConfig]
           [com.ontotext.trree OwlimSchemaRepository]))


(def default-repo-path "drafter-db")

;; Set these values later when we start the server
(def repo)
(def app)

(def worker)

(def state (atom {})) ; initialize state with an empty hashmap

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

(comment
  (defn load-graph-db-config [config-file]
    (let [config (statements config-file)
          config-graph (let [g (GraphImpl.)]  ;; Load config into a Graph
                         (add g config)
                         g)
          find-repo-nodes (fn [col] (filter (fn [q] ) config))
          lrm (LocalRepositoryManager. (io/file "my-repositories-10"))
          repo-nodes (->> config find-repo-nodes)]

      (.initialize lrm)

      (doseq [q config
              :when (or (and (= rdf:a (predicate q))
                             (= "http://www.openrdf.org/config/repository#Repository" (object q))))
              q2 config
              :when (and (= (subject q) (subject q2))
                         (= "http://www.openrdf.org/config/repository#repositoryID" (predicate q2)))]

        (log/info "Registering repo" (object q) "with id: " (subject q))
        (.addRepositoryConfig lrm (RepositoryConfig/create config-graph (URIImpl. (subject q)))))

      lrm))

  (def local-repo-manager (load-graph-db-config "graph-db.ttl")))

(defn make-native-sesame-repo [repo-path indexes]
  (let [repo (repo/repo (repo/native-store repo-path indexes))]
    (log/info "Initialised repo" repo-path)
    repo))

(defn make-graphdb-repo [repo-path indexes]
  (doto (SailRepository. (doto (OwlimSchemaRepository.)
                           (.setParameter "owlim-license" "SWIRRL_GRAPHDB_SE_expires-23-07-2015_latest-23-07-2015_16cores.license")
                           (.setParameter "storage-folder" repo-path)
                           (.setParameter "repository-type" "file-repository")
                           (.setParameter "enable-context-index" "true")))
    (.initialize)))

(defn initialise-repo! [repo-path indexes]
  (set-var-root! #'repo (make-graphdb-repo repo-path indexes) )

  (register-sparql-extension-functions))

(defroutes app-routes
  (route/resources "/")
  (route/not-found "Not Found"))

(defn initialise-app! [repo state]
  (set-var-root! #'app (app-handler
                        ;; add your application routes here
                        [(pages-routes repo)
                         (draft-api-routes "/draft" repo state)
                         (graph-management-routes "/graph" repo state)

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

(defn initialise-services! [repo-path indexes]
  (initialise-repo! repo-path indexes)
  (initialise-app! repo state))

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
                        (get env :drafter-indexes "spoc,posc,cosp"))

  (log/info "drafter started successfully"))

(defn destroy
  "destroy will be called when your application
   shuts down, put any clean up code here"
  []
  (log/info "drafter is shutting down.  Please wait (this can take a minute)...")
  (repo/shutdown repo)
  (future-cancel worker)
  (log/info "drafter has shut down."))
