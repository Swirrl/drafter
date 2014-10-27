(ns drafter.common.sparql-routes
  (:require [drafter.rdf.draft-management :refer [live-graphs]]))

(defn supplied-drafts
  "Parses out the set of \"graph\"s supplied on the request.

If no graphs are found in the request, it returns the set of live
  graphs.

This implementation does not enforce any security restrictions, and
  assumes that the client is trustworthy."
  [repo request]
  (let [graphs (-> request
                  :params
                  (get :graph))]

    (if graphs
      (if (instance? String graphs)
        #{graphs}
        graphs)

      (live-graphs repo))))
