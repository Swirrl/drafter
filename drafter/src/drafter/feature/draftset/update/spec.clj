(ns drafter.feature.draftset.update.spec
  (:require [clojure.spec.alpha :as s]
            [drafter.feature.draftset.update :as update]
            [drafter.time.spec]
            [drafter.time :as time]
            [drafter.draftset :as ds]
            [drafter.draftset.spec]
            [drafter.rdf.jena :as jena]
            [drafter.rdf.jena.spec]
            [drafter.manager :as manager]
            [drafter.manager.spec]
            [integrant.core :as ig]))

(s/def ::update/UpdateOperation #(satisfies? update/UpdateOperation %))

(s/def ::update/GraphState #{:unmanaged :live :draft})

(s/def ::update/draft-graph-uri uri?)
(s/def ::update/graph-uri uri?)
(s/def ::update/state ::update/GraphState)

(defn- nat? [n] (>= n 0))
;; NOTE: can't use nat-int? here since counts are not ints
(s/def ::update/draft-size (s/and integer? nat?))
(s/def ::update/live-size (s/and integer? nat?))

(s/def ::update/GraphMeta (s/keys :req-un [::update/draft-graph-uri
                                           ::update/graph-uri
                                           ::update/state
                                           ::update/draft-size
                                           ::update/live-size]))

(s/def ::update/GraphsMeta (s/map-of uri? ::update/GraphMeta))

(s/fdef update/get-graph-meta
  :args (s/cat :backend any? :draftset-ref any? :update-request ::jena/UpdateRequest)
  :ret ::update/GraphMeta)

;; update context
(s/def ::update/timestamp ::time/time)
(s/def ::update/rewriter fn?)
(s/def ::update/graph-manager any?)
(s/def ::update/max-update-size pos-int?)

(s/def ::update/UpdateContext (s/keys :req-un [::ds/draftset-ref
                                               ::update/timestamp
                                               ::update/graph-manager
                                               ::update/max-update-size]))

(s/def ::update/op ::update/UpdateOperation)

;; abstract operations
(defmulti abstract-operation-spec :type)

(defmethod abstract-operation-spec :create-new-draft [_op]
  (s/keys :req-un [::update/graph-uri ::update/draft-graph-uri]))

(defmethod abstract-operation-spec :create-live-draft [_op]
  (s/keys :req-un [::update/graph-uri ::update/draft-graph-uri]))

(defmethod abstract-operation-spec :clone [_op]
  (s/keys :req-un [::update/graph-uri ::update/draft-graph-uri]))

(defmethod abstract-operation-spec :rewrite [_op]
  (s/keys :req-un [::update/op]))

(defmethod abstract-operation-spec :rewrite-draftset [_op]
  (s/keys))

(defmethod abstract-operation-spec :touch [_op]
  (s/keys :req-un [::update/draft-graph-uri]))

(s/def ::update/AbstractOperation (s/multi-spec abstract-operation-spec :type))

(s/fdef update/reify-operation
  :args (s/cat :abstract-op ::update/AbstractOperation :update-context ::update/UpdateContext)
  :ret (s/coll-of ::jena/JenaUpdateOperation))

;; UpdateOperation
(s/fdef update/affected-graphs
  :args (s/cat :op ::update/UpdateOperation)
  :ret (s/coll-of ::update/graph-uri :kind set?))

(s/fdef update/size
  :args (s/cat :op ::update/UpdateOperation)
  :ret nat-int?)

(s/fdef update/abstract-operations
        :args (s/cat :op ::update/UpdateOperation :graph-meta ::update/GraphsMeta :max-update-size ::update/max-update-size)
  :ret (s/coll-of ::update/AbstractOperation))

(s/fdef update/rewrite
  :args (s/cat :op ::update/UpdateOperation :rewriter ::update/rewriter)
  :ret ::update/UpdateOperation)

;; update plan
(s/def ::update/operations (s/and (s/coll-of ::update/AbstractOperation)
                                  sequential?))
(s/def ::update/graph-meta ::update/GraphsMeta)

(s/def ::update/UpdatePlan (s/keys :req-un [::update/operations
                                            ::update/graph-meta]))

(s/fdef update/build-update
  :args (s/cat :update-plan ::update/UpdatePlan :update-context ::update/UpdateContext)
  :ret string?)

;; handler
(defmethod ig/pre-init-spec ::update/handler [_]
  (s/keys :req [:drafter/manager]
          :req-un [::wrap-as-draftset-owner ::max-update-size]))

