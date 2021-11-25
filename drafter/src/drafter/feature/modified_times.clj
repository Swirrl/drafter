(ns drafter.feature.modified-times
  (:require [drafter.rdf.sparql :as sparql]
            [drafter.rdf.sesame :as ses]
            [grafter.url :as url]
            [drafter.backend.draftset.draft-management :as mgmt]
            [grafter.vocabularies.dcterms :refer [dcterms:modified]]
            [clojure.java.io :as io]
            [grafter-2.rdf4j.repository :as repo]
            [drafter.rdf.drafter-ontology :refer [modified-times-graph-uri]]
            [drafter.backend.draftset.operations :as dsops]
            [drafter.backend.draftset.graphs :as graphs]
            [grafter-2.rdf4j.sparql :as sp]
            [clojure.string :as string]
            [drafter.backend :as backend]
            [drafter.backend.draftset :as backend-draftset]
            [drafter.util :as util]
            [clojure.data :refer [diff]]
            [drafter.rdf.jena :as jena]
            [grafter-2.rdf.protocols :as pr])
  (:import [org.eclipse.rdf4j.repository.sparql.query QueryStringUtil]))

(defn- get-update-query-with-bindings [query-source bindings]
  (let [q (slurp query-source)]
    (QueryStringUtil/getUpdateString q (ses/map->binding-set bindings))))

(defn- exec-update-with-bindings [conn query-source bindings]
  (let [q (get-update-query-with-bindings query-source bindings)]
    (sparql/update! conn q)))

(defn- update-draftset-timestamp-query [draftset-ref modified-at]
  (mgmt/set-timestamp (url/->java-uri draftset-ref) dcterms:modified modified-at))

(defn- update-draftset-version-query [draftset-ref]
  (mgmt/set-version (url/->java-uri draftset-ref) (util/version)))

(defn- update-draftset-timestamp! [conn draftset-ref modified-at]
  (sparql/update! conn (update-draftset-timestamp-query draftset-ref modified-at)))

(defn- update-draftset-version! [conn draftset-ref]
  (sparql/update! conn (update-draftset-version-query draftset-ref)))

(defn- update-draft-graph-modified [repo draftset-ref draft-modified-graph-uri draft-graph-uri modified-at]
  (let [bindings {:dmg draft-modified-graph-uri
                  :dg draft-graph-uri
                  :modified modified-at}
        q (io/resource "drafter/feature/modified_times/update-graph-modified-at.sparql")]
    (with-open [conn (repo/->connection repo)]
      (exec-update-with-bindings conn q bindings)
      (update-draftset-timestamp! conn draftset-ref modified-at)
      (update-draftset-version! conn draftset-ref))))

(defn draft-graph-appended! [repo draftset-ref draft-modified-graph-uri draft-graph-uri modified-at]
  (update-draft-graph-modified repo draftset-ref draft-modified-graph-uri draft-graph-uri modified-at))

(defn- delete-draft-graph-modified-time
  "Remove the modified timestamp for the given draft graph from a draft modifications graph"
  [repo draft-modified-graph-uri draft-graph-uri modified-at]
  (let [bindings {:dmg draft-modified-graph-uri
                  :dg draft-graph-uri
                  :modified modified-at}
        q (io/resource "drafter/feature/modified_times/revert-graph-modified-at.sparql")]
    (with-open [conn (repo/->connection repo)]
      (exec-update-with-bindings conn q bindings))))

(defn- has-remaining-user-graphs? [graph-manager live->draft]
  (boolean (some (fn [graph] (graphs/user-graph? graph-manager graph)) (keys live->draft))))

(defn- remove-draft-graph-modified [repo graph-manager live->draft draft-graph-uri removed-at]
  (let [has-user-graphs? (has-remaining-user-graphs? graph-manager live->draft)]
    (if-let [draft-modifications-graph (get live->draft modified-times-graph-uri)]
      (if has-user-graphs?
        (do
          (delete-draft-graph-modified-time repo draft-modifications-graph draft-graph-uri removed-at)
          live->draft)
        (do
          (mgmt/delete-draft-graph! repo draft-modifications-graph)
          (dissoc live->draft modified-times-graph-uri)))
      live->draft)))

(defn- draft-graph-removed
  "Indicates a draft graph was removed from a draftset. Removes the modification time from the draft modifications
   graph and deletes the modifications graph if the removed graph was the last user draft graph. Returns the new
   live->draft graph mapping of the draft after the modifications graph update. The removed draft graph must NOT
   exist in the live->draft graph mapping"
  [repo graph-manager draftset-ref live->draft draft-graph-uri removed-at]
  (let [live->draft (remove-draft-graph-modified repo graph-manager live->draft draft-graph-uri removed-at)]
    (with-open [conn (repo/->connection repo)]
      (update-draftset-timestamp! conn draftset-ref removed-at)
      (update-draftset-version! conn draftset-ref))
    live->draft))

(defn draft-graph-reverted!
  "Indicates a draft graph has been removed from a draftset at the given time and updates the draft modifications graph.
   The draft graph must NOT still exist in the state graph mapping for the draftset."
  [repo graph-manager draftset-ref draft-graph-uri modified-at]
  (let [live->draft (dsops/get-draftset-graph-mapping repo draftset-ref)]
    (draft-graph-removed repo graph-manager draftset-ref live->draft draft-graph-uri modified-at)
    nil))

(defn- ensure-draft-modifications-graph [graph-manager draftset-ref]
  (graphs/ensure-protected-graph-draft graph-manager draftset-ref modified-times-graph-uri))

(defn draft-graph-deleted! [repo graph-manager draftset-ref draft-graph-uri deleted-at]
  (let [dmg (ensure-draft-modifications-graph graph-manager draftset-ref)]
    (update-draft-graph-modified repo draftset-ref dmg draft-graph-uri deleted-at)
    nil))

(defn draft-only-graph-deleted! [repo graph-manager draftset-ref live->draft draft-graph-uri deleted-at]
  (draft-graph-removed repo graph-manager draftset-ref live->draft draft-graph-uri deleted-at))

(defn draft-graph-data-deleted! [repo draftset-ref draft-modifications-graph draft-graph-uri deleted-at]
  (update-draft-graph-modified repo draftset-ref draft-modifications-graph draft-graph-uri deleted-at)
  nil)

(defn query-modification-times
  "Returns a map of {graph uri -> modified time} for each graph in the modifications graph of the given query endpoint"
  [repo]
  (let [bindings (with-open [conn (repo/->connection repo)]
                   (vec (sp/query "drafter/feature/modified_times/get-graph-modified-times.sparql" conn)))]
    (into {} (map (juxt :lg :modified) bindings))))

(defn- publish-modified-times-query [{:keys [modified-graphs to-add] :as modification}]
  (let [inserts (map (fn [[live-graph modified]]
                       (format "<%s> dcterms:modified %s ." live-graph (mgmt/xsd-datetime modified)))
                     to-add)
        to-delete-graph-values (string/join " " (map #(str "<" % ">") modified-graphs))]
    (str
      "PREFIX dcterms: <http://purl.org/dc/terms/>"
      "DELETE {"
      "  GRAPH <" modified-times-graph-uri "> {"
      "    ?g dcterms:modified ?modified ."
      "  }"
      "} WHERE {"
      "  GRAPH <" modified-times-graph-uri "> {"
      "    VALUES ?g { " to-delete-graph-values " }"
      "    ?g dcterms:modified ?modified ."
      "  }"
      "} ;"
      "INSERT DATA {"
      "  GRAPH <" modified-times-graph-uri "> {"
      (string/join "\n" inserts)
      "  }"
      "}")))

(defn- delete-graph-modifications-query [dest-graph affected-graphs]
  (let [to-delete-graph-values (string/join " " (map #(str "<" % ">") affected-graphs))]
    (str
      "PREFIX dcterms: <http://purl.org/dc/terms/>"
      "DELETE {"
      "  GRAPH <" dest-graph "> {"
      "    ?g dcterms:modified ?modified ."
      "  }"
      "} WHERE {"
      "  GRAPH <" dest-graph "> {"
      "    VALUES ?g { " to-delete-graph-values " }"
      "    ?g dcterms:modified ?modified ."
      "  }"
      "}")))

(defn- insert-graph-modifications-query [dest-graph graph->modified]
  (let [inserts (map (fn [[graph modified]]
                       (pr/->Quad graph dcterms:modified modified dest-graph))
                     graph->modified)]
    (util/quads->insert-data-query inserts)))

(defn- remove-empty-draft-modifications-graph [draft-modifications-graph]
  (let [bindings {:dmg draft-modifications-graph}]
    (get-update-query-with-bindings (io/resource "drafter/feature/modified_times/remove_empty_draft_modifications_graph.sparql") bindings)))

(defn- remove-empty-draft-only-graft-modifications [draftset-ref draft-modifications-graph]
  (let [bindings {:ds  (url/->java-uri draftset-ref)
                  :dmg draft-modifications-graph}]
    (get-update-query-with-bindings (io/resource "drafter/feature/modified_times/delete_empty_draft_only_graph_modifications.sparql") bindings)))

(defn- get-modifications-graph-state [repo draftset-ref]
  (let [bindings (with-open [conn (repo/->connection repo)]
                   (vec (sp/query "drafter/feature/modified_times/get_modifications_graph_draft_state.sparql" {:ds (url/->java-uri draftset-ref)} conn)))]
    (case (count bindings)
      0 {:state :unmanaged :draft-graph-uri nil}
      1 (let [{:keys [public dg]} (first bindings)]
          {:state (if public :public :managed) :draft-graph-uri dg})
      (throw (ex-info "modifications query returned multiple results" {:bindings bindings})))))

(defn- create-draft-modifications-graph-update [graph-manager draftset-ref modifications-graph-state draft-modifications-graph now]
  (let [new-draft-statements (graphs/new-draft-protected-graph-statements graph-manager modified-times-graph-uri draft-modifications-graph now draftset-ref)
        quads (if (= :unmanaged modifications-graph-state)
                (concat new-draft-statements
                        (graphs/new-managed-protected-graph-statements graph-manager modified-times-graph-uri))
                new-draft-statements)]
    (jena/insert-data-stmt quads)))

(defn update-modifications-queries [repo graph-manager draftset-ref draft-user-graph-uris now]
  (let [modifications-graph-state (get-modifications-graph-state repo draftset-ref)
        [dmg-exists? draft-modifications-graph] (if-let [dmg (:draft-graph-uri modifications-graph-state)]
                                                  [true dmg]
                                                  [false (mgmt/make-draft-graph-uri)])
        draft-graph-uris (conj draft-user-graph-uris draft-modifications-graph)
        draft->modified (zipmap draft-graph-uris (repeat now))]
    (if dmg-exists?
      [(delete-graph-modifications-query draft-modifications-graph draft-graph-uris)
       (insert-graph-modifications-query draft-modifications-graph draft->modified)
       (remove-empty-draft-only-graft-modifications draftset-ref draft-modifications-graph)
       (remove-empty-draft-modifications-graph draft-modifications-graph)
       (update-draftset-timestamp-query draftset-ref now)
       (update-draftset-version-query draftset-ref)]
      [(create-draft-modifications-graph-update graph-manager draftset-ref (:state modifications-graph-state) draft-modifications-graph now)
       (insert-graph-modifications-query draft-modifications-graph draft->modified)
       (remove-empty-draft-only-graft-modifications draftset-ref draft-modifications-graph)
       (remove-empty-draft-modifications-graph draft-modifications-graph)
       (update-draftset-timestamp-query draftset-ref now)
       (update-draftset-version-query draftset-ref)])))

(defn publish-modified-times [repo graph->modified]
  (let [q (publish-modified-times-query graph->modified)]
    (with-open [conn (repo/->connection repo)]
      (sparql/update! conn q))))

(defn- merge-modifications [live-modifications draft-modifications published-at]
  (let [[_unmodified new-graphs modified-graphs]  (diff (set (keys live-modifications))
                                                        (set (keys draft-modifications)))
        new-modifications (merge-with (fn [live-modified draft-modified]
                                        (if (pos? (compare live-modified draft-modified))
                                          published-at
                                          draft-modified)) live-modifications draft-modifications)]
    {:modified-graphs modified-graphs
     :to-add new-modifications}))

(defn publish-modifications-graph [repo live->draft published-at]
  (when-let [draft-modifications-graph (get live->draft modified-times-graph-uri)]
    (let [live-repo (backend/live-endpoint-repo repo)
          draft-repo (backend-draftset/create-draftset-repo repo live->draft false)
          live-modification-times (query-modification-times live-repo)
          draft-modification-times (query-modification-times draft-repo)
          new-modifications (merge-modifications live-modification-times draft-modification-times published-at)
          queries [(publish-modified-times-query new-modifications)
                   (mgmt/set-isPublic-query modified-times-graph-uri true)
                   (mgmt/delete-draft-state-query draft-modifications-graph)
                   (mgmt/delete-graph-contents-query draft-modifications-graph)]
          compound-query (util/make-compound-sparql-query queries)]
      (with-open [conn (repo/->connection repo)]
        (sparql/update! conn compound-query)))))
