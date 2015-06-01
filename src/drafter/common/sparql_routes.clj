(ns drafter.common.sparql-routes
  (:require [clojure.set :as set]
            [drafter.rdf.draft-management :as mgmt :refer [live-graphs]]))

(defn supplied-drafts
  "Parses out the set of \"graph\"s supplied on the request.

  If no graphs are found in the request, it returns the set of live
  graphs.

  This implementation does not enforce any security restrictions, and
  assumes that the client is trustworthy."
  [repo {:keys [params] :as request}]

  (let [graphs (get params :graph)
        union-with-live? (get params :union-with-live false)
        draft-set (if (instance? String graphs)
                    #{graphs}
                    graphs)
        ;; get the graphs with drafts from graph-map
        graphs-with-drafts (into #{}
                                 (map str
                                      (keys (mgmt/graph-map repo draft-set))))
        live-graphs (if union-with-live?
                      (live-graphs repo)
                      #{})
        supplied-graphs (if (instance? String graphs)
                          #{graphs}
                          graphs)]
    (set/union
      (set/difference live-graphs graphs-with-drafts)
      supplied-graphs)))
