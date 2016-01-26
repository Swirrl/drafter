(ns drafter.common.sparql-routes
  (:require [clojure.set :as set]
            [drafter.util :refer [to-coll map-all]]
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
        supplied-draftset (to-coll graphs)
        graph-map (map-all str (mgmt/graph-map repo supplied-draftset))]
    (mgmt/graph-mapping->graph-restriction repo graph-map union-with-live?)))
