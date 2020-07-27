(ns drafter.feature.draftset.update
  (:require [clojure.set :as set]
            [clojure.spec.alpha :as s]
            [drafter.backend.draftset.arq :as arq]
            [drafter.backend.draftset.draft-management :as dm]
            [drafter.backend.draftset.operations :as ops]
            [drafter.rdf.drafter-ontology :refer :all]
            [drafter.draftset :as ds]
            [drafter.rdf.sparql :as sparql]
            [drafter.util :as util]
            [integrant.core :as ig]
            [ring.middleware.cors :as cors]
            [swirrl-server.errors :refer [wrap-encode-errors]]
            [grafter.vocabularies.rdf :refer :all])
  (:import org.apache.jena.graph.NodeFactory
           [org.apache.jena.sparql.algebra Algebra OpAsQuery]
           org.apache.jena.sparql.core.Quad
           [org.apache.jena.sparql.modify.request
            QuadDataAcc Target UpdateCopy UpdateDataDelete UpdateDataInsert]
           org.apache.jena.sparql.sse.SSE
           [org.apache.jena.update UpdateFactory UpdateRequest]))

;; TODO: size/batching?
;; TODO: sparql protocol handler for update
;; TODO: which graphs in data?
;; TODO: update graph metadata
;; TODO: copy-graph

(defn not-implemented []
  (throw (UnsupportedOperationException. "Not implemented")))

(defn- rewrite-quad [rewriter quad]
  (-> quad SSE/str arq/->sse-item arq/sse-zipper rewriter str SSE/parseQuad))

(defn- rewrite-query-pattern [rewriter query-pattern]
  (let [op (-> query-pattern
               Algebra/compile
               arq/->sse-item
               arq/sse-zipper
               rewriter
               str
               SSE/parseOp)]
    (-> op  OpAsQuery/asQuery .getQueryPattern)))

(defprotocol UpdateOperation
  (affected-graphs [op])
  (size [op])
  (rewrite [op rewriter]))

(extend-protocol UpdateOperation
  UpdateDataDelete
  (affected-graphs [op]
    (set (map #(.getURI (.getGraph %)) (.getQuads op))))
  (size [op]
    (count (.getQuads op)))
  (rewrite [op rewriter]
    (->> (.getQuads op)
         (map (partial rewrite-quad rewriter))
         (QuadDataAcc.)
         (UpdateDataDelete.)))

  UpdateDataInsert
  (affected-graphs [op]
    (set (map #(.getURI (.getGraph %)) (.getQuads op))))
  (size [op]
    (count (.getQuads op)))
  (rewrite1 [op rewriter]
    (->> (.getQuads op)
         (map (partial rewrite-quad rewriter))
         (QuadDataAcc.)
         (UpdateDataInsert.)))

  ;; TODO: Although this does a naïve rewrite of a DELETE/INSERT, we're not
  ;; supporting this operation until we can work out if it's possible to
  ;; gather sufficient info about its size, and which graphs it affects.
  ;; UpdateDeleteInsert
  ;; (rewrite [op rewriter]
  ;;   (let [where (-> op .getWherePattern rewrite-query-pattern)
  ;;         op' (UpdateDeleteInsert.)
  ;;         delete-acc (.getDeleteAcc op')
  ;;         insert-acc (.getInsertAcc op')]
  ;;     (doseq [delete (map rewrite-quad (.getDeleteQuads op))]
  ;;       (.addQuad delete-acc delete))
  ;;     (doseq [insert (map rewrite-quad (.getInsertQuads op))]
  ;;       (.addQuad insert-acc insert))
  ;;     (.setElement op' where)
  ;;     op'))

  )

(defn- within-limit? [operations {:keys [max-update-size] :as opts}]
  (< (reduce + (map size operations)) max-update-size))

(defn- processable? [operation]
  (satisfies? UpdateOperation operation))

(defn- unprocessable-request [unprocessable]
  (ex-info "422 Unprocessable Entity"
           {:status 422
            :headers {"Content-Type" "text/plain"}
            :body "422 Unprocessable Entity"}))

(defn- payload-too-large [operations]
  (ex-info "413 Payload Too Large."
           {:status 413
            :headers {"Content-Type" "text/plain"}
            :body "413 Payload Too Large."}))

(defn- parse-body [request opts]
  (let [update-request (-> request :body UpdateFactory/create)
        operations (.getOperations update-request)
        unprocessable (remove processable? operations)]
    (cond (seq unprocessable)
          (unprocessable-request unprocessable)
          (not (within-limit? operations opts))
          (payload-too-large operations)
          :else
          update-request)))

(defn- draftset-graphs [repo draftset-id]
  (let [ds-uri (ds/->draftset-uri draftset-id)
        q (str
           "SELECT ?lg WHERE { "
           (dm/with-state-graph
             "?ds <" rdf:a "> <" drafter:DraftSet "> ."
             "?dg <" drafter:inDraftSet "> ?ds ."
             "?lg <" rdf:a "> <" drafter:ManagedGraph "> ."
             "?lg <" drafter:hasDraft "> ?dg ."
             "VALUES ?ds { <" ds-uri "> }")
           "}")]
    (set (map :lg (sparql/eager-query repo q)))))

(defn- graph-copyable? [graph-uri]
  ;; (not-implemented)
  true)

(defn- ->jena-quad [{:keys [c s p o]}]
  (letfn [(->node [x]
            (if (uri? x)
              (NodeFactory/createURI (str x))
              (NodeFactory/createLiteral (str x))))]
    (let [[c s p o] (map ->node [c s p o])]
      (Quad. c s p o))))

(defn- insert-data-stmt [quads]
  (UpdateDataInsert. (QuadDataAcc. (map ->jena-quad quads))))

(defn- copy-graph-operation [draftset-uri timestamp graph-uri]
  (let [managed-graph-quads (dm/to-quads (dm/create-managed-graph graph-uri))
        draft-graph-uri (dm/make-draft-graph-uri)
        draft-graph-quads (apply dm/to-quads
                                 (dm/create-draft-graph graph-uri
                                                        draft-graph-uri
                                                        timestamp
                                                        draftset-uri))
        insert-data (insert-data-stmt (concat managed-graph-quads
                                              draft-graph-quads))
        copy-graph-stmt (UpdateCopy. (Target/create (str graph-uri))
                                     (Target/create (str draft-graph-uri)))]
    [insert-data copy-graph-stmt]))

(defn- copy-graphs-operations [draftset-id graphs-to-copy]
  (let [draftset-uri (ds/->draftset-uri draftset-id)
        now (util/get-current-time)]
    (mapcat (partial copy-graph-operation draftset-uri now) graphs-to-copy)))

(defn- prerequisites [repo draftset-id update-request]
  (let [affected-graphs (->> (.getOperations update-request)
                             (map affected-graphs)
                             (apply set/union))
        graphs-to-copy (set/difference affected-graphs
                                       (draftset-graphs repo draftset-id))]
    ;; TODO: Can we do this check in the query, and have it report decent errors?
    (if-let [uncopyable-graphs (seq (remove graph-copyable? graphs-to-copy))]
      (ex-info "Unable to copy graphs"
               {:status 500
                :headers {"Content-Type" "text/plain"}
                :body "500 Unable to copy graphs"})
      (copy-graphs-operations draftset-id graphs-to-copy))))

(defn- add-operations [update-request operations]
  (doseq [op operations] (.add update-request op))
  update-request)

(defn handler*
  [{:keys [drafter/backend wrap-as-draftset-owner] :as opts} request]
  (let [draftset-id (:draftset-id (:params request))
        update-request (parse-body request opts)]
    (if-let [error-response (ex-data update-request)]
      error-response
      ;; TODO: UpdateRequest base-uri, prefix, etc?
      (let [prerequisite-ops (prerequisites backend draftset-id update-request)
            operations' (->> (.getOperations update-request)
                             (map #(rewrite % rewriter)))
            update-request' (-> (UpdateRequest.)
                                (add-operations prerequisite-ops)
                                (add-operations operations'))]
        (println 'update-request' update-request')
        (sparql/update! backend (str update-request'))))))

(defn handler
  [{:keys [drafter/backend wrap-as-draftset-owner] :as opts}]
  (wrap-as-draftset-owner (fn [request] (handler* opts request))))

(s/def ::max-update-size integer?)

(defmethod ig/pre-init-spec ::handler [_]
  (s/keys :req [:drafter/backend]
          :req-un [::wrap-as-draftset-owner ::max-update-size]))

(def cors-allowed-headers
  #{"Accept"
    "Accept-Encoding"
    "Authorization"
    "Cache-Control"
    "Content-Type"
    "DNT"
    "If-Modified-Since"
    "Keep-Alive"
    "User-Agent"
    "X-CustomHeader"
    "X-Requested-With"})

(defmethod ig/init-key ::handler [_ opts]
  (-> (handler opts)
      (wrap-encode-errors)
      (cors/wrap-cors :access-control-allow-headers cors-allowed-headers
                      :access-control-allow-methods [:get :options :post]
                      :access-control-allow-origin #".*")))
