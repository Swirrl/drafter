(ns drafter.common.sparql-routes
  (:require [clojure.set :as set]
            [drafter.util :refer [to-coll]]
            [drafter.rdf.draft-management :as mgmt :refer [live-graphs]]))

(defn- calculate-graph-restriction [public-live-graphs live-graph-drafts supplied-draft-graphs]
  (set/union
   (set/difference public-live-graphs live-graph-drafts)
   supplied-draft-graphs))

(defn supplied-drafts
  "Parses out the set of \"graph\"s supplied on the request.

  If no graphs are found in the request, it returns the set of live
  graphs.

  This implementation does not enforce any security restrictions, and
  assumes that the client is trustworthy."
  [repo {:keys [params] :as request}]

  (let [graphs (get params :graph)
        union-with-live? (get params :union-with-live false)
        supplied-draftset (to-coll graphs)
        ;; get the graphs with drafts from graph-map
        graphs-with-drafts (into #{}
                                 (map str
                                      (keys (mgmt/graph-map repo supplied-draftset))))

        public-live-graphs (if union-with-live?
                             (live-graphs repo)
                             #{})]

    (calculate-graph-restriction public-live-graphs
                                 graphs-with-drafts
                                 supplied-draftset)))
