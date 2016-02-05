(ns drafter.rdf.draftset-management
  (:require [grafter.vocabularies.rdf :refer :all]
            [grafter.rdf :refer [add s]]
            [grafter.rdf.protocols :refer [update!]]
            [grafter.rdf.repository :refer [query]]
            [drafter.rdf.drafter-ontology :refer :all]
            [drafter.util :as util]
            [drafter.rdf.draft-management :refer [to-quads with-state-graph drafter-state-graph] :as mgmt]
            [clojure.string :as string])
  (:import [java.net URI]
           [java.util Date UUID]))

(defprotocol DraftsetRef
  (->draftset-uri [this])
  (->draftset-id [this]))

(defrecord DraftsetURI [uri]
  Object
  (toString [this] uri))

(defrecord DraftsetId [id]
  DraftsetRef
  (->draftset-uri [this] (->DraftsetURI (drafter.rdf.drafter-ontology/draftset-uri id)))
  (->draftset-id [this] this)

  Object
  (toString [this] id))

(extend-type DraftsetURI
  DraftsetRef
  (->draftset-uri [this] this)
  (->draftset-id [{:keys [uri]}]
    (let [base-uri (URI. (drafter.rdf.drafter-ontology/draftset-uri ""))
          relative (.relativize base-uri (URI. uri))]
      (->DraftsetId (.toString relative)))))

(defn- create-draftset-statements [username title description draftset-uri created-date]
  (let [ss [draftset-uri
            [rdf:a drafter:DraftSet]
            [drafter:createdAt created-date]
            [drafter:createdBy (s username)]
            [drafter:hasOwner (s username)]]
        ss (util/conj-if (some? title) ss [rdfs:label (s title)])]
    (util/conj-if (some? description) ss [rdfs:comment (s description)])))

(defn create-draftset!
  "Creates a new draftset in the given database and returns its id. If
  no title is provided (i.e. it is nil) a default title will be used
  for the new draftset."
  ([db creator] (create-draftset! db creator nil))
  ([db creator title] (create-draftset! db creator title nil))
  ([db creator title description] (create-draftset! db creator title description (UUID/randomUUID) (Date.)))
  ([db creator title description draftset-id created-date]
   (let [template (create-draftset-statements (:email creator) title description (draftset-uri draftset-id) created-date)
         quads (to-quads template)]
     (add db quads)
     (->DraftsetId (str draftset-id)))))

(defn- draftset-exists-query [draftset-ref]
  (str "ASK WHERE {"
       (with-state-graph
         "<" (->draftset-uri draftset-ref) "> <" rdf:a "> <" drafter:DraftSet ">")
       "}"))

(defn draftset-exists? [db draftset-ref]
  (let [q (draftset-exists-query draftset-ref)]
    (query db q)))

(defn- delete-statements-for-subject-query [graph-uri subject-uri]
  (str "DELETE { GRAPH <" graph-uri "> { <" subject-uri "> ?p ?o } } WHERE {"
       "  GRAPH <" graph-uri "> { <" subject-uri "> ?p ?o }"
       "}"))

(defn- delete-draftset-statements-query [draftset-ref]
  (let [ds-uri (str (->draftset-uri draftset-ref))]
    (delete-statements-for-subject-query drafter-state-graph ds-uri)))

(defn delete-draftset-statements! [db draftset-ref]
  (let [delete-query (delete-draftset-statements-query draftset-ref)]
    (grafter.rdf.protocols/update! db delete-query)))

(defn- get-draftset-graph-mapping-query [draftset-ref]
  (let [ds-uri (str (->draftset-uri draftset-ref))]
    (str
     "SELECT ?lg ?dg WHERE { "
     (with-state-graph
       "<" ds-uri "> <" rdf:a "> <" drafter:DraftSet "> ."
       "?dg <" drafter:inDraftSet "> <" ds-uri "> ."
       "?lg <" rdf:a "> <" drafter:ManagedGraph "> ."
       "?lg <" drafter:hasDraft "> ?dg .")
     "}")))

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
(defn get-draftset-graph-mapping [repo draftset-ref]
  (let [mapping-query (get-draftset-graph-mapping-query draftset-ref)
        results (query repo mapping-query)]
    (graph-mapping-result-seq->map results)))

(defn- graph-mapping-draft-graphs [graph-mapping]
  (vals graph-mapping))

(defn- delete-draftset-query [draftset-ref draft-graph-uris]
  (let [delete-drafts-query (map mgmt/delete-draft-graph-and-remove-from-state-query draft-graph-uris)
        delete-draftset-query (delete-draftset-statements-query draftset-ref)]
    (util/make-compound-sparql-query (conj delete-drafts-query delete-draftset-query))))

(defn delete-draftset! [db draftset-ref]
  (let [graph-mapping (get-draftset-graph-mapping db draftset-ref)
        draft-graphs (graph-mapping-draft-graphs graph-mapping)
        delete-query (delete-draftset-query draftset-ref draft-graphs)]
    (update! db delete-query)))

;;Repository -> Map {DraftSetURI -> {String String}}
(defn- get-all-draftset-graph-mappings [repo]
  (let [results (query repo (get-all-draftset-graph-mappings-query))
        draftset-grouped-results (group-by #(get % "ds") results)]
    (into {} (map (fn [[ds-uri mappings]]
                    [(.stringValue ds-uri) (graph-mapping-result-seq->map mappings)])
                  draftset-grouped-results))))

(defn- get-draftset-properties-query [draftset-ref]
  (let [draftset-uri (str (->draftset-uri draftset-ref))]
    (str
     "SELECT * WHERE { "
     (with-state-graph
       "<" draftset-uri "> <" rdf:a "> <" drafter:DraftSet "> ."
       "<" draftset-uri "> <" drafter:createdAt "> ?created ."
       "OPTIONAL { <" draftset-uri "> <" rdfs:comment "> ?description . }"
       "OPTIONAL { <" draftset-uri "> <" rdfs:label "> ?title }")
     "}")))

(defn- get-all-draftsets-properties-query []
  (str
   "SELECT * WHERE { "
   (with-state-graph
     "?ds <" rdf:a "> <" drafter:DraftSet "> ."
     "?ds <" drafter:createdAt "> ?created ."
     "OPTIONAL { ?ds <" rdfs:comment "> ?description . }"
     "OPTIONAL { ?ds <" rdfs:label "> ?title . }")
   "}"))

(defn- calendar-literal->date [literal]
  (.. literal (calendarValue) (toGregorianCalendar) (getTime)))

(defn- draftset-properties-result->properties [draftset-ref {:strs [created title description]}]
  (let [required-fields {:id (str (->draftset-id draftset-ref))
                         :created-at (calendar-literal->date created)}
        optional-fields {:display-name (and title (.stringValue title))
                         :description (and description (.stringValue description))}]
    (merge required-fields (remove (comp nil? second) optional-fields))))

(defn- get-draftset-properties [repo draftset-ref]
  (let [properties-query (get-draftset-properties-query draftset-ref)
        results (query repo properties-query)]
    (if-let [result (first results)]
      (draftset-properties-result->properties draftset-ref result))))

(defn- combine-draftset-properties-and-graphs [properties graph-mapping]
  (let [live-graph-info (util/map-values (constantly {}) graph-mapping)]
      (assoc properties :data live-graph-info)))

(defn get-draftset-info [repo draftset-ref]
  (if-let [ds-properties (get-draftset-properties repo draftset-ref)]
    (let [ds-graph-mapping (get-draftset-graph-mapping repo draftset-ref)]
      (combine-draftset-properties-and-graphs ds-properties ds-graph-mapping))))

(defn get-all-draftsets-info [repo]
  (let [all-properties (query repo (get-all-draftsets-properties-query))
        all-graph-mappings (get-all-draftset-graph-mappings repo)
        all-infos (map (fn [{ds-uri "ds" :as result}]
                         (let [ds-uri (.stringValue ds-uri)
                               properties (draftset-properties-result->properties (->DraftsetURI ds-uri) result)
                               graph-mapping (get all-graph-mappings ds-uri)]
                           (combine-draftset-properties-and-graphs properties graph-mapping)))
                       all-properties)]
    all-infos))

(defn delete-draftset-graph! [db draftset-ref graph-uri]
  (when (mgmt/is-graph-managed? db graph-uri)
    (let [graph-mapping (get-draftset-graph-mapping db draftset-ref)]
      (if-let [draft-graph-uri (get graph-mapping graph-uri)]
        (do
          (mgmt/delete-graph-contents! db draft-graph-uri)
          draft-graph-uri)
        (mgmt/create-draft-graph! db graph-uri {} (str (->draftset-uri draftset-ref)))))))

(def ^:private draftset-param->predicate
  {:display-name rdfs:label
   :description rdfs:comment})

(defn- set-draftset-metadata-query [draftset-uri po-pairs]
  (str
   "DELETE {"
   (with-state-graph
     "<" draftset-uri "> ?p ?o .")
   "} INSERT {"
   (with-state-graph
     (string/join " " (map (fn [[p o]] (str "<" draftset-uri "> <" p "> \"" o "\" .")) po-pairs)))
   "} WHERE {"
   (with-state-graph
     "VALUES ?p { " (string/join " " (map #(str "<" (first %) ">") po-pairs)) " }"
     "OPTIONAL { <" draftset-uri "> ?p ?o . }")
   "}"))

(defn set-draftset-metadata!
  "Takes a map containing new values for various metadata keys and
  updates them on the given draftset."
  [backend draftset-ref meta-map]
  (when-let [update-pairs (vals (util/intersection-with draftset-param->predicate meta-map vector))]
    (let [q (set-draftset-metadata-query (->draftset-uri draftset-ref) update-pairs)]
      (update! backend q))))
