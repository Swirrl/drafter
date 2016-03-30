(ns drafter.routes.sparql
  (:require [compojure.core :refer [context defroutes routes routing let-request
                                    make-route let-routes
                                    ANY GET POST PUT DELETE HEAD]]
            [drafter.rdf.draft-management :as mgmt]
            [drafter.backend.protocols :refer :all]
            [drafter.rdf.sparql-protocol :refer [sparql-end-point process-sparql-query wrap-sparql-errors]]
            [drafter.rdf.endpoints :refer [live-endpoint state-endpoint]]
            [clojure.tools.logging :as log]
            [swirrl-server.responses :as r]
            [drafter.util :refer [to-coll]]
            [swirrl-server.errors :refer [encode-error]]))

(defn- draft-query-endpoint [executor request timeouts]
  (try
    (let [{{:keys [union-with-live graph]} :params} request
          live->draft (log/spy (mgmt/graph-map executor (to-coll graph)))
          rewriting-executor (create-rewriter executor live->draft (or union-with-live false))]

      (process-sparql-query rewriting-executor request
                            :query-timeouts timeouts))))

(defmethod encode-error :multiple-drafts-error [ex]
  (r/error-response 412 :multiple-drafts-error (.getMessage ex)))

(defn draft-sparql-routes
  ([mount-point executor] (draft-sparql-routes mount-point executor nil))
  ([mount-point executor timeouts]
     (wrap-sparql-errors
      (routes
       (GET mount-point request
            (draft-query-endpoint executor request timeouts))

       (POST mount-point request
             (draft-query-endpoint executor request timeouts))))))

(defn live-sparql-routes [mount-point executor timeouts]
  (sparql-end-point mount-point (live-endpoint executor) timeouts))

(defn state-sparql-routes [mount-point executor timeouts]
  (sparql-end-point mount-point (state-endpoint executor) timeouts))

(defn raw-sparql-routes [mount-point executor timeouts]
  (sparql-end-point mount-point executor timeouts))
