(ns drafter.test-generators
  (:require [drafter.rdf.drafter-ontology :refer :all]
            [grafter.vocabularies.rdf :refer [rdf:a rdfs:label rdfs:comment]]
            [drafter.rdf.draft-management :refer [drafter-state-graph create-draft-graph]]
            [drafter.rdf.draftset-management :refer [create-draftset-statements]]
            [drafter.rdf.draft-management :refer [to-quads]]
            [grafter.rdf :refer [add]]
            [grafter.rdf.templater :refer [graph]]
            [grafter.rdf.protocols :as proto]
            [grafter.rdf.repository :as repo]
            [clojure.test.check.generators :as gen]
            [drafter.util :as util]
            [drafter.user-test :refer [test-editor test-publisher test-manager]]
            [drafter.user :as user])
  (:import [java.util UUID Date]
           [java.net URI]))

(defn- make-prefixed-uri-gen [uri-prefix]
  (gen/fmap #(URI. (str uri-prefix %)) gen/uuid))

(def draftset-uri-gen (gen/fmap (comp draftset-id->uri str) gen/uuid))
(def draft-graph-uri-gen (gen/fmap #(URI. (str "http://publishmydata.com/graphs/drafter/draft/" %)) gen/uuid))
(def managed-graph-uri-gen (gen/fmap #(URI. (str "http://live/" %)) gen/uuid))
(def subject-gen (gen/fmap #(URI. (str "http://subject/" %)) gen/uuid))
(def predicate-gen (gen/fmap #(URI. (str "http://predicate/" %)) gen/uuid))
(def object-uri-gen (make-prefixed-uri-gen "http://object/"))
(def object-gen (gen/one-of [object-uri-gen gen/string-alphanumeric gen/boolean]))
(def triple-gen (gen/fmap (fn [[s p o]] (proto/->Quad s p o nil)) (gen/tuple subject-gen predicate-gen object-gen)))
(def user-gen (gen/elements [test-editor test-publisher test-manager]))

(defn make-triples-gen [v]
  (cond (number? v) (gen/vector triple-gen v)
        (= ::gen v) (gen/vector triple-gen 0 5)
        :else (gen/return v)))

(def date-gen (gen/let [y (gen/choose 100 120)
                        m (gen/choose 0 11)
                        d (gen/choose 1 28)]
                       (Date. y m d)))

(defn make-draft-graph-gen
  ([] (make-draft-graph-gen ::gen))
  ([spec] (make-draft-graph-gen spec nil))
  ([{:keys [draftset-uri triples created] :as spec} ds-uri-gen]
   (if (= ::gen spec)
     (make-draft-graph-gen {:triples ::gen} ds-uri-gen)
     (let [ds-uri-gen (if (nil? draftset-uri) (or ds-uri-gen draftset-uri-gen) (gen/return draftset-uri))
           created-gen (if (nil? created) date-gen (gen/return created))]
       (gen/hash-map :draftset-uri ds-uri-gen
                     :triples (make-triples-gen triples)
                     :created created-gen)))))

(defn- gen-pairs->map-gen
  "Converts a sequence of [key-gen value-gen] pairs into a generator for maps. Each generator pair
   will be used to construct a key-value pair in the generated map"
  [gen-pairs]
  (gen/fmap #(into {} %) (apply gen/tuple gen-pairs)))

(defn- make-drafts-gen
  ([v] make-drafts-gen v nil)
  ([v ds-uri-gen]
   (cond
     (number? v) (gen/map draft-graph-uri-gen (make-draft-graph-gen ::gen ds-uri-gen) {:num-elements v})
     (= ::gen v) (gen/map draft-graph-uri-gen (make-draft-graph-gen ::gen ds-uri-gen) {:max-elements 5})
     (map? v) (let [gen-pairs (map (fn [[k v]] (gen/tuple (gen/return k) (make-draft-graph-gen v ds-uri-gen))) v)]
                (gen-pairs->map-gen gen-pairs))
     (nil? v) (gen/return {})
     :else (throw (RuntimeException. "Expected ::gen, map, nil or number of drafts in drafts spec")))))

(defn- make-managed-graph-gen
  ([spec] (make-managed-graph-gen spec nil))
  ([spec ds-uri-gen]
   (cond
     (= ::gen spec) (make-managed-graph-gen {:triples ::gen :drafts ::gen} ds-uri-gen)
     (map? spec) (let [{:keys [is-public triples drafts]} spec
                       is-public-gen (if (nil? is-public) gen/boolean (gen/return is-public))]
                   (gen/hash-map :is-public is-public-gen
                                 :triples (make-triples-gen triples)
                                 :drafts (make-drafts-gen drafts ds-uri-gen)))
     :else (throw (RuntimeException. "Expected ::gen or map for managed graph spec")))))

(defn- make-managed-graphs-gen
  ([spec] (make-managed-graphs-gen spec nil))
  ([spec ds-uri-gen]
   (cond
     (number? spec) (gen/map managed-graph-uri-gen (make-managed-graph-gen ::gen ds-uri-gen) {:num-elements spec})
     (= ::gen spec) (gen/map managed-graph-uri-gen (make-managed-graph-gen ::gen ds-uri-gen) {:max-elements 5})
     (map? spec) (let [gen-pairs (map (fn [[k v]] (gen/tuple (gen/return k) (make-managed-graph-gen v ds-uri-gen))) spec)]
                   (gen-pairs->map-gen gen-pairs))
     (nil? spec) (make-managed-graphs-gen {} ds-uri-gen)
     :else (throw (RuntimeException. "Expected ::gen, nil, number or map for managed graphs spec")))))

(def role-gen (gen/elements user/roles))

(defn- make-submission-gen [spec]
  (cond (= ::gen spec) (gen/one-of [(gen/hash-map :user user-gen)
                                    (gen/hash-map :role role-gen)])
        (map? spec) (let [{:keys [user role]} spec]
                      (cond
                        (and (some? user) (some? role))
                        (throw (RuntimeException. "Only one of user or role can be specified for draftset submission"))

                        ;;TODO: allow ::gen for user or role?
                        (some? user) (gen/return {:user user})
                        (some? role) (gen/return {:role role})

                        :else (throw (RuntimeException. "user or role required for draftset submission"))))
        :else (throw (RuntimeException. "Expected ::gen or map for draftset submission spec"))))

(defn- make-draftset-gen [{:keys [created-by owned-by submission created-at title description id] :as spec}]
  (let [id-gen (if (some? id) (gen/return id) gen/uuid)
        uri-gen (gen/fmap draftset-id->uri id-gen)
        created-by-gen (if (some? created-by) (gen/return created-by) user-gen)
        created-at-gen (if (some? created-at) (gen/return created-at) date-gen)
        owned-by-gen (gen/return owned-by)
        submission-gen (if (some? submission) (make-submission-gen submission) (gen/return nil))]
    (gen/hash-map :id id-gen
                  :uri uri-gen
                  :title (gen/return title)
                  :description (gen/return description)
                  :created-at created-at-gen
                  :created-by created-by-gen
                  :owned-by owned-by-gen
                  :submission submission-gen)))

(defn- make-draftsets-gen [spec]
  (cond (number? spec) (gen/vector (make-draftset-gen {}) spec)
        (= ::gen spec) (gen/vector (make-draftset-gen {}) 1 5)
        (coll? spec) (apply gen/tuple (map make-draftset-gen spec))
        (nil? spec) (make-draftsets-gen 1)))

(comment {:managed-graphs {"http://woo" {:is-public false
                                         :triples []
                                         :drafts {"http://draft1" {:draftset-uri "http://draftset1"}
                                                  "http://draft2" {:draftset-uri "http://draft2"
                                                                   :triples []
                                                                   :created (Date.)}}}}
          :draftsets [{:uri "http://draftset1"
                       :created-by test-editor
                       :owned-by test-publisher
                       :submission {:role :publisher :user test-manager}
                       :title "Title"
                       :description "description"
                       :id (UUID/randomUUID)
                       :created-at (Date.)
                       :modified-at (Date.)}]})

(defn- submission-statements [submission-uri {:keys [role user]}]
  (let [po (if (some? role)
             [drafter:claimRole (name role)]
             [drafter:claimUser (user/user->uri user)])
        template [submission-uri
                  [rdf:a drafter:Submission]
                  po]]
    (to-quads template)))

(defn- draftset-statements [{:keys [uri created-by owned-by created-at submission modified-at title description]}]
  (let [user-uri (user/user->uri created-by)
        template [uri
                  [rdf:a drafter:DraftSet]
                  [drafter:createdAt created-at]
                  [drafter:modifiedAt (or modified-at created-at)]
                  [drafter:createdBy user-uri]]
        template (util/conj-if (some? title) template [rdfs:label title])
        template (util/conj-if (some? description) template [rdfs:comment description])]
    (if (some? submission)
      (let [submission-uri (submission-id->uri (UUID/randomUUID))
            submission-quads (submission-statements submission-uri submission)
            template (conj template
                           [drafter:hasSubmission submission-uri]
                           [drafter:submittedBy user-uri]   ;TODO: allow submitting user to be specified
                           )]
        (concat (to-quads template) submission-quads))
      (let [owned-by (or owned-by created-by)
            template (conj template [drafter:hasOwner (user/user->uri owned-by)])]
        (to-quads template)))))

(defn- draftsets-statements [draftsets]
  (mapcat draftset-statements draftsets))

(defn- draft-graph-statements [managed-graph-uri draft-graph-uri {:keys [draftset-uri triples created]}]
  ;;TODO: add modified time to draft graph spec?
  (let [created-at (or created (Date.))
        state-quads (apply graph drafter-state-graph (create-draft-graph managed-graph-uri draft-graph-uri created-at draftset-uri))
        graph-quads (map #(assoc % :c draft-graph-uri) triples)]
    (concat state-quads graph-quads)))

(defn- managed-graph-statements [graph-uri {:keys [is-public triples drafts]}]
  (let [state-quads (graph drafter-state-graph [graph-uri
                                                [rdf:a drafter:ManagedGraph]
                                                [drafter:isPublic is-public]])
        graph-quads (map #(assoc % :c graph-uri) triples)
        draft-quads (mapcat (fn [[dg dg-spec]] (draft-graph-statements graph-uri dg dg-spec)) drafts)]
    (concat state-quads graph-quads draft-quads)))

(defn- managed-graphs-statements [graphs]
  (mapcat (fn [[uri def]] (managed-graph-statements uri def)) graphs))

(defn make-spec-gen [spec]
  (let [draftsets-gen (make-draftsets-gen (:draftsets spec))
        draftsets (gen/generate draftsets-gen)
        draftset-uris (map :uri draftsets)
        ds-uri-gen (if (seq draftset-uris) (gen/elements draftset-uris) draftset-uri-gen)]
    (gen/hash-map :draftsets (gen/return draftsets)
                  :managed-graphs (make-managed-graphs-gen (:managed-graphs spec) ds-uri-gen))))

(defn generate-statements [spec]
  (let [spec-gen (make-spec-gen spec)
        {:keys [draftsets managed-graphs]} (gen/generate spec-gen)
        ds-statements (draftsets-statements draftsets)
        mg-statements (managed-graphs-statements managed-graphs)]
    (concat ds-statements mg-statements)))

(defn generate-in [repository spec]
  (add repository (generate-statements spec))
  repository)

(defn generate-repository [spec]
  (generate-in (repo/repo) spec))
