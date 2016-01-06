(ns drafter.rdf.draftset-management
  (:require [grafter.vocabularies.rdf :refer :all]
            [grafter.rdf :refer [add s]]
            [grafter.rdf.repository :refer [query]]
            [drafter.rdf.drafter-ontology :refer :all]
            [drafter.util :as util]
            [drafter.rdf.draft-management :refer [to-quads with-state-graph drafter-state-graph]])
  (:import [java.net URI]
           [java.util Date UUID]))

(defn- create-draftset-statements [title description draftset-uri created-date]
  (let [base-quads [draftset-uri
                    [rdf:a drafter:DraftSet]
                    [rdfs:label (s title)]
                    [drafter:createdAt created-date]]]
    (util/conj-if (some? description) base-quads [rdfs:comment (s description)])))

(defn create-draftset!
  "Creates a new draftset in the given database and returns its id. If
  no title is provided (i.e. it is nil) a default title will be used
  for the new draftset."
  ([db title] (create-draftset! db title nil))
  ([db title description] (create-draftset! db title description (UUID/randomUUID) (Date.)))
  ([db title description draftset-id created-date]
   (let [template (create-draftset-statements title description (draftset-uri draftset-id) created-date)
         quads (to-quads template)]
     (add db quads)
     draftset-id)))

(defn- delete-statements-for-subject-query [graph-uri subject-uri]
  (str "DELETE { GRAPH <" graph-uri "> { <" subject-uri "> ?p ?o } } WHERE {"
       "  GRAPH <" graph-uri "> { <" subject-uri "> ?p ?o }"
       "}"))

(defn delete-draftset-statements! [db draftset-uri]
  (let [delete-query (delete-statements-for-subject-query drafter-state-graph draftset-uri)]
    (grafter.rdf.protocols/update! db delete-query)))

(defn- get-draftset-graph-mapping-query [draftset-uri]
  (str
   "SELECT ?lg ?dg WHERE { "
   (with-state-graph
     "<" draftset-uri "> <" rdf:a "> <" drafter:DraftSet "> ."
     "?dg <" drafter:inDraftSet "> <" draftset-uri "> ."
     "?lg <" rdf:a "> <" drafter:ManagedGraph "> ."
     "?lg <" drafter:hasDraft "> ?dg .")
   "}"))

(defn- get-all-draftset-graph-mappings-query []
  (str
   "SELECT * WHERE { "
   (with-state-graph
     "?ds <"  rdf:a "> <" drafter:DraftSet "> ."
     "?dg <" drafter:inDraftSet "> ?ds ."
     "?lg <" rdf:a "> <" drafter:ManagedGraph "> ."
     "?lg <" drafter:hasDraft "> ?dg .")
   "}"))

;;seq {"lg" URI "dg" URI} -> {String String}
(defn- graph-mapping-result-seq->map [mapping-results]
  (into {} (map (fn [{:strs [lg dg]}] [(.stringValue lg) (.stringValue dg)]) mapping-results)))

;;Repository -> String -> Map {String String}
(defn get-draftset-graph-mapping [repo draftset-uri]
  (let [mapping-query (get-draftset-graph-mapping-query draftset-uri)
        results (query repo mapping-query)]
    (graph-mapping-result-seq->map results)))

;;Repository -> Map {DraftSetURI -> {String String}}
(defn- get-all-draftset-graph-mappings [repo]
  (let [results (query repo (get-all-draftset-graph-mappings-query))
        draftset-grouped-results (group-by #(get % "ds") results)]
    (into {} (map (fn [[ds-uri mappings]]
                    [(.stringValue ds-uri) (graph-mapping-result-seq->map mappings)])
                  draftset-grouped-results))))

(defn- get-draftset-properties-query [draftset-uri]
  (str
   "SELECT * WHERE { "
   (with-state-graph
     "<" draftset-uri "> <" rdf:a "> <" drafter:DraftSet "> ."
     "<" draftset-uri "> <" rdfs:label "> ?title ."
     "<" draftset-uri "> <" drafter:createdAt "> ?created ."
     "OPTIONAL { <" draftset-uri "> <" rdfs:comment "> ?description }")
   "}"))

(defn- get-all-draftsets-properties-query []
  (str
   "SELECT * WHERE { "
   (with-state-graph
     "?ds <" rdf:a "> <" drafter:DraftSet "> ."
     "?ds <" rdfs:label "> ?title ."
     "?ds <" drafter:createdAt "> ?created ."
     "OPTIONAL { ?ds <" rdfs:comment "> ?description }")
   "}"))

(defn- calendar-literal->date [literal]
  (.. literal (calendarValue) (toGregorianCalendar) (getTime)))

(defn- draftset-uri->id [draftset-uri]
  (let [base-uri (URI. (drafter.rdf.drafter-ontology/draftset-uri ""))
        relative (.relativize base-uri (URI. draftset-uri))]
    (.toString relative)))

(defn- draftset-properties-result->properties [draftset-id {:strs [created title description]}]
  (util/conj-if (some? description)
                {:display-name (.stringValue title)
                 :created-at (calendar-literal->date created)
                 :id draftset-id}
                [:description (.stringValue description)]))

(defn- get-draftset-properties [repo draftset-uri]
  (let [draftset-id (draftset-uri->id draftset-uri)
        properties-query (get-draftset-properties-query draftset-uri)
        results (query repo properties-query)]
    (if-let [result (first results)]
      (draftset-properties-result->properties draftset-id result))))

(defn- combine-draftset-properties-and-graphs [properties graph-mapping]
  (let [live-graph-info (util/map-values (constantly {}) graph-mapping)]
      (assoc properties :data live-graph-info)))

(defn get-draftset-info [repo draftset-uri]
  (if-let [ds-properties (get-draftset-properties repo draftset-uri)]
    (let [ds-graph-mapping (get-draftset-graph-mapping repo draftset-uri)]
      (combine-draftset-properties-and-graphs ds-properties ds-graph-mapping))))

(defn get-all-draftsets-info [repo]
  (let [all-properties (query repo (get-all-draftsets-properties-query))
        all-graph-mappings (get-all-draftset-graph-mappings repo)
        all-infos (map (fn [{ds-uri "ds" :as result}]
                         (let [ds-uri (.stringValue ds-uri)
                               draftset-id (draftset-uri->id ds-uri)
                               properties (draftset-properties-result->properties draftset-id result)
                               graph-mapping (get all-graph-mappings ds-uri)]
                           (combine-draftset-properties-and-graphs properties graph-mapping)))
                       all-properties)]
    all-infos))
