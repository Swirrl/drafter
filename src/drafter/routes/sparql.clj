(ns drafter.routes.sparql
  (:require [compojure.core :refer [context defroutes routes routing let-request
                                    make-route let-routes
                                    ANY GET POST PUT DELETE HEAD]]
            [drafter.rdf.draft-management :as mgmt]
            [drafter.backend.protocols :refer :all]
            [drafter.rdf.sparql-protocol :refer [sparql-end-point process-sparql-query wrap-sparql-errors]]
            [clojure.tools.logging :as log]
            [swirrl-server.responses :as r]
            [swirrl-server.errors :refer [encode-error]]
            [drafter.common.sparql-routes :refer [supplied-drafts]]))

(defn- draft-query-endpoint [executor request timeouts]
  (try
    (let [{:keys [params]} request
          graph-uris (supplied-drafts executor request)
          live->draft (log/spy (mgmt/graph-map executor graph-uris))
          rewriting-executor (create-rewriter executor live->draft)]

      (process-sparql-query rewriting-executor request
                            :graph-restrictions graph-uris
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
  (sparql-end-point mount-point executor (partial mgmt/live-graphs executor) timeouts))

(defn state-sparql-routes [mount-point executor timeouts]
  (sparql-end-point mount-point executor #{mgmt/drafter-state-graph} timeouts))

(defn raw-sparql-routes [mount-point executor timeouts]
  (sparql-end-point mount-point executor nil timeouts))
