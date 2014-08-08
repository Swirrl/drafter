(ns drafter.routes.sparql
  (:require [compojure.core :refer [context defroutes routes routing let-request
                                    make-route let-routes
                                    ANY GET POST PUT DELETE HEAD]]
            [ring.util.io :as io]
            [compojure.route :refer [not-found]]
            [compojure.handler :refer [api]]
            [drafter.rdf.draft-management :as mgmt]
            [drafter.rdf.sparql-protocol :refer [sparql-end-point process-sparql-query]]
            [drafter.rdf.sparql-rewriting :as rew]
            [grafter.rdf.sesame :as ses])
  (:import [org.openrdf.query.resultio TupleQueryResultFormat BooleanQueryResultFormat]))

(defn supplied-drafts
  "Parses out the set of \"graph\"s supplied on the request.

Returns a function that when called with a single argument (the
  database which may be ignored) will return a set of named graphs.

If no graphs are found in the request, a function that returns the set
of live graphs is returned."
  [repo request]
  (let [graphs (-> request
                  :query-params
                  (get "graph"))]
    (if graphs
      (if (instance? String graphs)
        #{graphs}
        graphs)

      (mgmt/live-graphs repo))))

(defn make-draft-query-rewriter [draft-uris]
  (fn [repo query-str]
    (println "draft uris" draft-uris)
    (let [mapping (mgmt/graph-map repo draft-uris)]
      (println  "Using mapping: " mapping)
      (rew/rewrite-graph-query repo query-str mapping))))

(defn- draft-query-endpoint [repo request]
  (let [graph-uris (supplied-drafts repo request)]
    (process-sparql-query repo request
                          :query-creator-fn (make-draft-query-rewriter graph-uris)
                          :graph-restrictions graph-uris)))


(defn draft-sparql-routes [repo]
  (routes
   (GET "/sparql/draft" request
        (draft-query-endpoint repo request))

   (POST "/sparql/draft" request
         (draft-query-endpoint repo request))))

(defn live-sparql-routes [repo]
  (sparql-end-point "/sparql/live" repo (mgmt/live-graphs repo)))

(defn state-sparql-routes [repo]
  (sparql-end-point "/sparql/state" repo #{mgmt/drafter-state-graph}))
