(ns drafter.test-generators
  (:require [drafter.rdf.drafter-ontology :refer :all]
            [grafter.vocabularies.rdf :refer [rdf:a]]
            [drafter.rdf.draft-management :refer [drafter-state-graph create-draft-graph]]
            [grafter.rdf :refer [add]]
            [grafter.rdf.templater :refer [graph]]
            [grafter.rdf.protocols :as proto]
            [grafter.rdf.repository :as repo])
  (:import [java.util UUID Random Date]
           [java.net URI]))

(defn- gen-draftset-uri [rand]
  (draftset-id->uri (str (UUID/randomUUID))))

(defn- gen-draft-graph-uri [rand]
  (URI. (str "http://publishmydata.com/graphs/drafter/draft/" (UUID/randomUUID))))

(defn- gen-managed-graph-uri [rand]
  (URI. (str "http://live/" (UUID/randomUUID))))

(defn- gen-subject [rand]
  (URI. (str "http://subject/" (UUID/randomUUID))))

(defn- gen-predicate [rand]
  (URI. (str "http://predicate/" (UUID/randomUUID))))

(defn- gen-object [rand]
  (case (.nextInt rand 3)
    0 (URI. (str "http://object/" (UUID/randomUUID)))
    1 (str "object-" (UUID/randomUUID))
    (.nextBoolean rand)))

(defn- gen-triple [rand]
  (proto/->Quad (gen-subject rand) (gen-predicate rand) (gen-object rand) nil))

(defn- gen-n-triples [n rand]
  (vec (repeatedly n #(gen-triple rand))))

(defn- gen-triples [rand]
  (gen-n-triples (.nextInt rand 5) rand))

(defn- gen-draft-graph-draftset-uri [managed-graph-uri draft-graph-uri {:keys [draftsets] :as state} rand]
  (let [draftset-uris (keys draftsets)]
    (if (>= (count draftset-uris) 5)
      (assoc-in state [:managed-graphs managed-graph-uri :drafts draft-graph-uri :draftset-uri] (nth draftset-uris (.nextInt rand 5)))
      (let [ds-uri (gen-draftset-uri rand)]
        (-> state
            (assoc-in [:managed-graphs managed-graph-uri :drafts draft-graph-uri :draftset-uri] ds-uri)
            (update :draftsets #(assoc % ds-uri {:draft-graphs [draft-graph-uri]})))))))

(defn- ensure-draftset-draft-graph [draftset-uri draft-graph-uri state]
  (let [path [:draftsets draftset-uri :draft-graphs]]
    (if-let [ds-draft-graphs (get-in state path)]
      (if (#{draft-graph-uri} ds-draft-graphs)
        state
        (update-in state path conj draft-graph-uri))
      (assoc-in state path [draft-graph-uri]))))

(defn- gen-draft-graph-triples [managed-graph-uri draft-graph-uri state rand]
  (let [{:keys [triples] :as ds-state} (get-in state [:managed-graphs managed-graph-uri :drafts draft-graph-uri])]
    (cond
      (number? triples)
      (assoc-in state [:managed-graphs managed-graph-uri :drafts draft-graph-uri :triples] (gen-n-triples triples rand))

      (contains? ds-state :triples)
      state

      (or (nil? triples) (= ::gen triples))
      (assoc-in state [:managed-graphs managed-graph-uri :drafts draft-graph-uri :triples] (gen-triples rand)))))

(defn- gen-managed-draft-graph [managed-graph-uri draft-graph-uri state rand]
  (let [draft-state (get-in state [:managed-graphs managed-graph-uri :drafts draft-graph-uri])]
    (cond
      (= ::gen draft-state)
      (gen-managed-draft-graph managed-graph-uri draft-graph-uri (update-in state [:managed-graphs managed-graph-uri :drafts] dissoc draft-graph-uri) rand)

      (nil? draft-state)
      (let [ds-state (gen-draft-graph-draftset-uri managed-graph-uri draft-graph-uri state rand)]
        (gen-draft-graph-triples managed-graph-uri draft-graph-uri ds-state rand))

      (map? draft-state)
      (let [with-ds (if (contains? draft-state :draftset-uri)
                      (ensure-draftset-draft-graph (:draftset-uri draft-state) draft-graph-uri state)
                      (gen-draft-graph-draftset-uri managed-graph-uri draft-graph-uri state rand))]
        (gen-draft-graph-triples managed-graph-uri draft-graph-uri with-ds rand)))))

(defn- gen-num-draft-graphs [managed-graph-uri n state rand]
  (reduce (fn [s _] (gen-managed-draft-graph managed-graph-uri (gen-draftset-uri rand) s rand)) state (range n)))

(defn- gen-managed-graph-draft-graphs [managed-graph-uri state rand]
  (let [drafts (get-in state [:managed-graphs managed-graph-uri :drafts])]
    (cond
      (number? drafts)
      (gen-num-draft-graphs managed-graph-uri drafts (update-in state [:managed-graphs managed-graph-uri] #(dissoc % :drafts)) rand)

      (or (nil? drafts) (= drafts ::gen))
      (gen-num-draft-graphs managed-graph-uri (.nextInt rand 5) (update-in state [:managed-graphs managed-graph-uri] #(dissoc % :drafts)) rand)

      (map? drafts)
      (reduce (fn [s dg] (gen-managed-draft-graph managed-graph-uri dg s rand)) state (keys drafts)))))

(defn- gen-boolean [rand]
  (.nextBoolean rand))

(defn- gen-managed-graph [managed-graph-uri state rand]
  (let [path [:managed-graphs managed-graph-uri]
        {:keys [triples] :as graph-state} (get-in state path)
        is-public (get graph-state :is-public (gen-boolean rand))
        triples (cond
                  (number? triples) (gen-n-triples triples rand)
                  (or (nil? triples) (= ::gen triples)) (gen-triples rand)
                  :else triples)
        tmp (assoc-in state path {:is-public is-public
                                  :triples   triples
                                  :drafts    (get-in state (conj path :drafts))})]
    (gen-managed-graph-draft-graphs managed-graph-uri tmp rand)))

(defn- gen-managed-graphs [state rand]
  (let [graphs-state (:managed-graphs state)]
    (cond
      (number? graphs-state)
      (let [mg-uris (repeatedly graphs-state #(gen-managed-graph-uri rand))]
        (reduce (fn [s mg] (gen-managed-graph mg s rand)) (dissoc state :managed-graphs) mg-uris))

      (or (nil? graphs-state) (= ::gen graphs-state))
      (gen-managed-graphs (assoc state :managed-graphs (inc (.nextInt rand 5))) rand)

      (map? graphs-state)
      (reduce (fn [s mg] (gen-managed-graph mg s rand)) state (keys graphs-state)))))

(defn- gen-draftsets [state rand]
  (let [ds-state (:draftsets state)]
    (cond
      (number? ds-state)
      (let [ds-uris (repeatedly ds-state #(gen-draftset-uri rand))
            ds-map (into {} (map (fn [uri] [uri {:draft-graphs []}]) ds-uris))]
        (assoc state :draftsets ds-map))

      (or (nil? ds-state) (= ::gen ds-state))
      (gen-draftsets (assoc state :draftsets (.nextInt rand 4)) rand)

      :else state)))

(defn- draftset-statements [draftsets]
  ;;TODO: include owner, creator and metadata in draftsets
  (map #(proto/->Quad % rdf:a drafter:DraftSet drafter-state-graph) (keys draftsets)))

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

(defn generate [spec]
  (let [r (Random.)]
    (gen-managed-graphs (gen-draftsets spec r) r)))

(defn ->statements [spec]
  (let [ds-statements (draftset-statements (:draftsets spec))
        mg-statements (mapcat (fn [[uri mg-spec]] (managed-graph-statements uri mg-spec)) (:managed-graphs spec))]
    (concat ds-statements mg-statements)))

(defn generate-statements [spec]
  (->statements (generate spec)))

(defn generate-repository [spec]
  (let [r (repo/repo)]
    (add r (generate-statements spec))
    r))

(comment {:managed-graphs {"http://woo" {:is-public false
                                         :triples []
                                         :drafts {"http://draft1" {:draftset-uri "http://draftset1"}
                                                  "http://draft2" {:draftset-uri "http://draft2"
                                                                   :triples []
                                                                   :created (Date.)}}}}
          :draftsets {"http://draftset1" {:draft-graphs ["http://draft2"]}}})