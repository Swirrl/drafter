(ns drafter.common.sparql-routes
  (:require [drafter.rdf.draft-management :refer [live-graphs]]
            [clojure.set :as set]))

(defn- maybe-merge-with-live [repo union-with-live graphs]
  (let [live-graphs (when union-with-live
                       (live-graphs repo))
        supplied-graphs (if (instance? String graphs)
                          #{graphs}
                          graphs)]
    (set/union live-graphs
               supplied-graphs)))

(defn supplied-drafts
  "Parses out the set of \"graph\"s supplied on the request.

If no graphs are found in the request, it returns the set of live
  graphs.

This implementation does not enforce any security restrictions, and
  assumes that the client is trustworthy."
  [repo {:keys [params] :as request}]
  (let [graphs (get params :graph)
        union-with-live (get params :union-with-live false)]

    (maybe-merge-with-live repo union-with-live graphs)
    ;;(live-graphs repo)
    ))
