(ns drafter.backend.draftset.graphs
  (:require [integrant.core :as ig]
            [grafter.vocabularies.rdf :refer :all]
            [drafter.rdf.drafter-ontology :refer :all]
            [drafter.backend.draftset.draft-management :as mgmt]
            [drafter.rdf.sparql :as sparql]
            [drafter.draftset :as ds]
            [drafter.backend.draftset.operations :as dsops]
            [drafter.util :as util]
            [grafter.url :as url]
            [drafter.time :as time])
  (:import [java.util.regex Pattern]
           [java.net URI]))

(def ^:private default-protected-graphs #{#"http://publishmydata.com/graphs/drafter/.*"})

(defprotocol URIMatcher
  "Protocol for matching URIs against a specification"
  (uri-matches? [this uri]))

(extend-protocol URIMatcher
  Pattern
  (uri-matches? [this uri]
    (boolean (re-matches this (str uri))))

  URI
  (uri-matches? [this uri]
    (= this uri)))

(defn create-manager
  "Creates a graph manager with an optional collection of additional protected
   graph specifications. Each element in the input collection should implement
   the URIMatcher protocol. A graph is considered to be protected if it matches
   any of either the default or specified matchers."
  ([repo] (create-manager repo #{}))
  ([repo protected-graphs] (create-manager repo protected-graphs time/system-clock))
  ([repo protected-graphs clock]
   {:repo repo
    :protected-graphs (into default-protected-graphs protected-graphs)
    :clock clock}))

(defn protected-graph?
  "Whether the given graph URI is protected"
  [{:keys [protected-graphs] :as manager} graph-uri]
  (->> protected-graphs
       (some (fn [m] (uri-matches? m graph-uri)))
       (boolean)))

(defn- check-graph-unprotected!
  "Throws an exception if the given live graph URI represents a protected graph"
  [manager live-graph-uri]
  (when (protected-graph? manager live-graph-uri)
    (throw (ex-info (str "Cannot create draft of protected graph " live-graph-uri)
                    {:error :protected-graph-modification-error
                     :graph-uri live-graph-uri}))))

(defn- check-graph-protected!
  [manager live-graph-uri]
  (when-not (protected-graph? manager live-graph-uri)
    (throw (ex-info (format "Expected graph %s to be protected" live-graph-uri)
                    {:error :protected-graph-confusion
                     :graph-uri live-graph-uri}))))

(defn- new-managed-graph-statements [graph-uri]
  (mgmt/to-quads
    [graph-uri
     [rdf:a drafter:ManagedGraph]
     [drafter:isPublic false]]))

(defn new-managed-user-graph-statements
  "Returns RDF statements representing a new managed user graph. Throws an exception
   if the URI is not valid for user graphs."
  [manager graph-uri]
  (check-graph-unprotected! manager graph-uri)
  (new-managed-graph-statements graph-uri))

(defn new-draft-user-graph-statements
  "Returns RDF statements representing a new draft graph for the user graph
   URI. Throws an exception if the URI is not valid for user graphs."
  [manager live-graph-uri draft-graph-uri time draftset-uri]
  (check-graph-unprotected! manager live-graph-uri)
  (let [live-graph-triples [live-graph-uri
                            [drafter:hasDraft draft-graph-uri]]
        draft-graph-triples [draft-graph-uri
                             [rdf:a drafter:DraftGraph]
                             [drafter:createdAt time]
                             [drafter:modifiedAt time]
                             [drafter:version (util/urn-uuid)]]
        draft-graph-triples (cond-> draft-graph-triples
                                    (some? draftset-uri) (conj [drafter:inDraftSet draftset-uri]))]
    (apply mgmt/to-quads [live-graph-triples draft-graph-triples])))

(defn- ensure-managed-graph
  "Ensures a managed graph exists for the specified URI. Callers should check the
   graph URI is a valid user/protected graph as required."
  [{:keys [repo] :as manager} graph-uri]
  ;; We only do anything if it's not already a managed graph

  ;; FIXME: Is this a potential race condition? i.e. we check for existence (and it's false) and before executing someone else makes
  ;; the managed graph(?). Ideally, we'd do this as a single INSERT/WHERE statement.
  (when-not (mgmt/is-graph-managed? repo graph-uri)
    (let [managed-graph-quads (new-managed-graph-statements graph-uri)]
      (sparql/add repo managed-graph-quads)))
  graph-uri)

(defn ensure-managed-user-graph
  "Ensures a managed graph exists for the given user graph URI. Throws an exception if
   the graph needs to be created but is not a valid user graph."
  [manager graph-uri]
  (check-graph-unprotected! manager graph-uri)
  (ensure-managed-graph manager graph-uri))

(defn- draft-graph-statements
  [live-graph-uri draft-graph-uri time draftset-uri]
  (let [live-graph-triples [live-graph-uri
                            [drafter:hasDraft draft-graph-uri]]
        draft-graph-triples [draft-graph-uri
                             [rdf:a drafter:DraftGraph]
                             [drafter:createdAt time]
                             [drafter:modifiedAt time]
                             [drafter:version (util/urn-uuid)]
                             [drafter:inDraftSet draftset-uri]]]
    (apply mgmt/to-quads [live-graph-triples draft-graph-triples])))

(defn- create-draft-graph
  "Creates a new draft graph with a unique graph name, expects the
  live graph to already be created. Returns the URI of the draft that
  was created."
  [{:keys [repo clock] :as manager} draftset-ref live-graph-uri]
  (let [now (time/now clock)
        draft-graph-uri (mgmt/make-draft-graph-uri)
        draftset-uri (url/->java-uri draftset-ref)
        quads (draft-graph-statements live-graph-uri draft-graph-uri now draftset-uri)]
    (sparql/add repo quads)
    draft-graph-uri))

(defn create-user-graph-draft
  "Creates a draft for a user (i.e. not protected) graph URI. Throws an exception
   if the specified graph URI is protected."
  [manager draftset-ref user-graph-uri]
  (ensure-managed-user-graph manager user-graph-uri)
  (create-draft-graph manager draftset-ref user-graph-uri))

(defn create-protected-graph-draft
  "Creates a draft for a protected graph URI. Throws an exception if the graph URI
   is not protected."
  [manager draftset-ref protected-graph-uri]
  (check-graph-protected! manager protected-graph-uri)
  (ensure-managed-graph manager protected-graph-uri)
  (create-draft-graph manager draftset-ref protected-graph-uri))

(defn delete-user-graph
  "Marks a user graph for deletion in live by removing its contents within a draft
   and returns the draft graph URI. If the graph doesn't exist in the draftset an
   empty draft graph is created for it, publishing the empty graph will then result
   in a deletion from live. Throws an exception if the graph-uri is not a valid
   user graph."
  [{:keys [repo clock] :as manager} draftset-ref graph-uri]
  (check-graph-unprotected! manager graph-uri)
  (when (mgmt/is-graph-managed? repo graph-uri)
    (let [graph-mapping (dsops/get-draftset-graph-mapping repo draftset-ref)
          modified-at (time/now clock)]
      (if-let [draft-graph-uri (get graph-mapping graph-uri)]
        (do
          (mgmt/delete-graph-contents! repo draft-graph-uri modified-at)
          (mgmt/unrewrite-draftset! repo {:draftset-uri (ds/->draftset-uri draftset-ref)
                                          :live-graph-uris [graph-uri]})
          draft-graph-uri)
        (create-draft-graph manager draftset-ref graph-uri)))))

(defmethod ig/init-key ::manager [_ {:keys [repo protected-graphs ::time/clock] :as opts}]
  (create-manager repo protected-graphs clock))
