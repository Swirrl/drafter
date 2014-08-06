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

(defn draft-query-rewriter [repo draft-uris]
  (fn [query-str]
    (doto
        (rew/rewrite-graph-query repo query-str (mgmt/graph-map repo draft-uris))
      (.setDataset nil))))

(defn draft-sparql-routes [repo]
  (routes
   (GET "/sparql/draft" request
        (let [graph-uris (supplied-drafts repo request)]
          (process-sparql-query repo request
                                :query-creator-fn (draft-query-rewriter repo graph-uris)
                                :graph-restrictions (supplied-drafts repo request))))

   (POST "/sparql/draft" request
         (process-sparql-query repo request
                               :query-creator-fn draft-query-rewriter
                               :graph-restrictions (supplied-drafts repo request)))))

(defn live-sparql-routes [repo]
  (sparql-end-point "/sparql/live" repo (mgmt/live-graphs repo)))

(defn state-sparql-routes [repo]
  (sparql-end-point "/sparql/state" repo #{mgmt/drafter-state-graph}))
