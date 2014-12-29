(ns drafter.pages.query-page
  (:require [drafter.rdf.drafter-ontology :refer [draft-uri drafter:DraftGraph drafter:hasDraft]]
            [drafter.rdf.draft-management :refer [drafter-state-graph]]
            [grafter.rdf.repository :refer [query ->connection]]
            [clojure.walk :refer [keywordize-keys]]
            [net.cgrand.enlive-html :as en]
            [environ.core :refer [env]]))

(en/defsnippet query-checkbox "dist/html/drafter.html" [:section :#queryform [:div.checkbox en/first-child]]
  [graph-uri]
  [:input] (en/do->
            (en/set-attr :value graph-uri))
  [[en/text-node (en/left [:input])]] (constantly graph-uri))

(en/defsnippet query-form "dist/html/drafter.html" [:section :#queryform]
  [action-uri graph-uris]
  [:form] (en/do->
           (en/set-attr :action action-uri))
  [:.form_group] (en/content (map query-checkbox graph-uris)))

(en/defsnippet query-page "dist/html/drafter.html" [:#contents :section]
  [heading {:keys [action-uri graph-uris]}]
  [:h2] (en/content heading)
  [:form] (en/content (query-form action-uri graph-uris)))

(defn parse-guid [uri]
  (.replace (str uri) (draft-uri "") ""))

(defn map-values [f m]
  (into {} (for [[k v] m] [k (f v)])))

(defn all-drafts [db]
  (doall (->> (query db (str
                         "SELECT ?draft ?live WHERE {"
                         "   GRAPH <" drafter-state-graph "> {"
                         "     ?draft a <" drafter:DraftGraph "> . "
                         "     ?live <" drafter:hasDraft "> ?draft . "
                         "   }"
                         "}"))
              (map keywordize-keys)
              (map (partial map-values str))
              (map (fn [m] (assoc m :guid (parse-guid (:draft m))))))))
