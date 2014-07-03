(ns drafter.routes.sparql
  (:require [compojure.core :refer [context defroutes routes routing let-request
                                    make-route let-routes
                                    ANY GET POST PUT DELETE HEAD]]
            [ring.util.io :as io]
            [compojure.route :refer [not-found]]
            [compojure.handler :refer [api]])
  (:require [drafter.rdf.draft-management :as mgmt]
            [drafter.rdf.sparql-protocol :refer [sparql-end-point process-sparql-query]]
            [grafter.rdf.sesame :as ses])
  (:import [org.openrdf.query.resultio TupleQueryResultFormat BooleanQueryResultFormat]))

(defn supplied-drafts
  "Parses out the set of \"graph\"s supplied on the request.

Returns a function that when called with a single argument (the
  database which may be ignored) will return a set of named graphs.

If no graphs are found in the request, a function that returns the set
of live graphs is returned."
  [request]
  (let [graphs (-> request
                  :query-params
                  (get "graph"))]
    (if graphs
      (constantly
       (if (instance? String graphs)
         #{graphs}
         graphs))

      mgmt/live-graphs)))

(defn draft-sparql-routes [repo]
  (routes
   (GET "/sparql/draft" request
        (process-sparql-query repo request (supplied-drafts request)))
   (POST "/sparql/draft" request
         (process-sparql-query repo request (supplied-drafts request)))))

(defn live-sparql-routes [repo]
  (sparql-end-point "/sparql/live" repo mgmt/live-graphs))

(defn state-sparql-routes [repo]
  (sparql-end-point "/sparql/state" repo (constantly #{mgmt/drafter-state-graph})))
