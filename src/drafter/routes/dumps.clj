(ns drafter.routes.dumps
  (:require [ring.util.io :as rio]
            [clojure.string :as str]
            [grafter.rdf.repository :as repo]
            [drafter.rdf.draft-management :as mgmt]
            [drafter.rdf.sparql-protocol :as sparql]
            [drafter.routes.sparql :as sp]
            [compojure.core :refer [let-request]]
            [clojure.tools.logging :as log]
            [ring.middleware.accept :refer [wrap-accept]]))

(defn dumps-endpoint
  "Implemented as a ring middle-ware dumps-endpoint wraps an existing
  ring sparql endpoint-handler and feeds it a construct query on the
  graph indicated by the graph-uri request parameter.

  As it's implemented as a middleware you can use it to provide dumps
  functionality on a query-rewriting endpoint, a restricted
  endpoint (e.g. only drafter public/live graphs) a raw endpoint, or
  any other."
  [endpoint-handler]
  (fn [request]
    (let [{graph-uri :graph-uri} (:params request)]
      (if-not graph-uri
        {:status 500 :body "You must supply a graph-uri parameter to specify the graph to dump."}

        (let [construct-request (assoc-in request
                                          [:params :query]
                                          (str "CONSTRUCT { ?s ?p ?o . } WHERE { GRAPH <" graph-uri "> { ?s ?p ?o . } }"))]

          (endpoint-handler construct-request))))))
