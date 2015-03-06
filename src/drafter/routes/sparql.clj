(ns drafter.routes.sparql
  (:require [compojure.core :refer [context defroutes routes routing let-request
                                    make-route let-routes
                                    ANY GET POST PUT DELETE HEAD]]
            [clojure.set :as set]
            [ring.util.io :as io]
            [compojure.route :refer [not-found]]
            [drafter.rdf.draft-management :as mgmt]
            [drafter.rdf.sparql-protocol :refer [sparql-end-point process-sparql-query result-handler-wrapper wrap-sparql-errors]]
            [drafter.rdf.sparql-rewriting :as rew]
            [clojure.tools.logging :as log]
            [grafter.rdf.repository :as ses]
            [drafter.common.sparql-routes :refer [supplied-drafts]])
  (:import [org.openrdf.query.resultio TupleQueryResultFormat BooleanQueryResultFormat]
           [org.openrdf.query.parser ParsedBooleanQuery ParsedGraphQuery ParsedTupleQuery]
           [org.openrdf.query.impl MapBindingSet]
           [org.openrdf.rio RDFHandler RDFWriter]))

(defn- draft-query-endpoint [repo request]
  (try
    (let [{:keys [params]} request
          graph-uris (log/spy :info  (supplied-drafts repo request))
          {:keys [result-rewriter query-rewriter]} (rew/make-draft-query-rewriter repo graph-uris)]

      (process-sparql-query repo request
                            :query-creator-fn query-rewriter
                            :result-rewriter result-rewriter
                            :graph-restrictions graph-uris))

    (catch clojure.lang.ExceptionInfo ex
      (let [unpack #(= %1 (-> %2 ex-data :error))
            status (condp unpack ex
                     :multiple-drafts-error 412)]
        {:status status :body (.getMessage ex)}))))

(defn draft-sparql-routes [mount-point repo]
  (wrap-sparql-errors
   (routes
    (GET mount-point request
         (draft-query-endpoint repo request))

    (POST mount-point request
          (draft-query-endpoint repo request)))))

(defn live-sparql-routes [mount-point repo]
  (sparql-end-point mount-point repo (partial mgmt/live-graphs repo)))

(defn state-sparql-routes [mount-point repo]
  (sparql-end-point mount-point repo #{mgmt/drafter-state-graph}))

(defn raw-sparql-routes [mount-point repo]
  (sparql-end-point mount-point repo))
