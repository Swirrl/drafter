(ns drafter.feature.draftset.update
  (:require [clojure.set :as set]
            [clojure.spec.alpha :as s]
            [drafter.backend.draftset.arq :as arq]
            [drafter.backend.draftset.draft-management :as dm]
            [drafter.backend.draftset.operations :as ops]
            [drafter.backend.draftset.rewrite-query :refer [uri-constant-rewriter]]
            [drafter.rdf.drafter-ontology :refer :all]
            [drafter.draftset :as ds]
            [drafter.rdf.sparql :as sparql]
            [drafter.util :as util]
            [integrant.core :as ig]
            [ring.middleware.cors :as cors]
            [swirrl-server.errors :refer [wrap-encode-errors]]
            [grafter.vocabularies.rdf :refer :all]
            [grafter.url :as url])
  (:import java.net.URI
           org.apache.jena.graph.NodeFactory
           [org.apache.jena.sparql.algebra Algebra OpAsQuery]
           org.apache.jena.sparql.core.Quad
           [org.apache.jena.sparql.modify.request
            QuadDataAcc Target
            UpdateCopy UpdateDrop UpdateDataDelete UpdateDeleteInsert UpdateDataInsert]
           org.apache.jena.sparql.sse.SSE
           org.apache.jena.sparql.sse.Item
           [org.apache.jena.update UpdateFactory UpdateRequest]))

;; TODO: drop graph
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

(defn- update-op [& q-strs]
  (let [ops (->> q-strs (apply str) UpdateFactory/create .getOperations)]
    (assert (= 1 (count ops)))
    (first ops)))

(defn- ->jena-quad [{:keys [c s p o]}]
  (letfn [(->node [x]
            (if (uri? x)
              (NodeFactory/createURI (str x))
              (NodeFactory/createLiteral (str x))))]
    (let [[c s p o] (map ->node [c s p o])]
      (Quad. c s p o))))

(defn- drop-live-graph-op [draftset-uri graph-uri]
  (let [op (update-op "
INSERT { } WHERE { GRAPH <" dm/drafter-state-graph "> {
  <" graph-uri "> <" rdf:a "> <" drafter:ManagedGraph ">
} FILTER NOT EXISTS { GRAPH <" dm/drafter-state-graph "> {
    <" graph-uri "> <" drafter:inDraftSet "> <" draftset-uri ">
  } }
}")
        insert-acc (.getInsertAcc op)]
    (doseq [quad (->> (dm/create-draft-graph graph-uri
                                             (dm/make-draft-graph-uri)
                                             (util/get-current-time)
                                             draftset-uri)
                      (apply dm/to-quads)
                      (map ->jena-quad))]
      (.addQuad insert-acc quad))
    op))

(defn- drop-draft-graph-op [draftset-uri graph-uri]
  "DROP GRAPH "
  )

(println (drop-live-graph-op "http://ds" "http://g"))

(defn- manage-graph-op [graph-uri]
  (update-op "
INSERT { GRAPH <" dm/drafter-state-graph "> {
  <" graph-uri "> <" rdf:a "> <" drafter:ManagedGraph "> .
  <" graph-uri "> <" drafter:isPublic "> false .
  }
} WHERE {
  FILTER NOT EXISTS {
    GRAPH <" dm/drafter-state-graph "> {
     <" graph-uri "> <" rdf:a "> <" drafter:ManagedGraph ">
    }
  }
}"))

(defn- empty-draft-graph-for-op [draftset-uri timestamp graph-uri]
  (let [draft-graph-uri (dm/make-draft-graph-uri)]
    (->> (dm/create-draft-graph graph-uri draft-graph-uri timestamp draftset-uri)
         (apply dm/to-quads)
         (map ->jena-quad)
         (QuadDataAcc.)
         (UpdateDataInsert.))))

;; TODO: need to know whether a graph is:
;; a) Just in live (not being copied)
;; b) Already copied to draftset
;; c) Only in current update


(defprotocol UpdateOperation
  (affected-graphs [op])
  (size [op])
  (raw-operations [op rewriter draftset-uri graph-meta])
  (rewrite [op rewriter]))

(defn just-in-live? [g meta]
  (let [{:keys [live? draft?]} (get meta g)]
    (and live? (not draft?))))

(defn in-draftset? [g meta]
  (let [{:keys [live? draft?]} (get meta g)]
    draft?))

(extend-protocol UpdateOperation
  UpdateDataDelete
  (affected-graphs [op]
    (set (map #(URI. (.getURI (.getGraph %))) (.getQuads op))))
  (size [op]
    (count (.getQuads op)))
  (rewrite [op rewriter]
    (->> (.getQuads op)
         (map (partial rewrite-quad rewriter))
         (QuadDataAcc.)
         (UpdateDataDelete.)))

  UpdateDataInsert
  (affected-graphs [op]
    (set (map #(URI. (.getURI (.getGraph %))) (.getQuads op))))
  (size [op]
    (count (.getQuads op)))
  (raw-operations [op rewriter draftset-uri graph-meta]
    ;; do we need to copy?
    )
  (rewrite [op rewriter]
    (->> (.getQuads op)
         (map (partial rewrite-quad rewriter))
         (QuadDataAcc.)
         (UpdateDataInsert.)))

  UpdateDrop
  (affected-graphs [op]
    #{;(URI. (.getURI (.getGraph op)))
      })
  (size [op] 1)
  (raw-operations [op rewriter draftset-uri graph-meta]
    (let [g (URI. (.getURI (.getGraph op)))]
      (cond (just-in-live? g graph-meta)
            [(empty-draft-graph-for-op draftset-uri (util/get-current-time) g)]
            ;; what if we insert an empty graph and then insert stuff follows it?
            (in-draftset? g graph-meta)
            ;; TODO: if-not (too-big? g)
            [(rewrite op rewriter)]
            ;; else throw? too large?
            :nowhere
            [(rewrite op rewriter)]
            ;; TODO: is this right? we're either going to be deleting nothing -
            ;; OK, or deleting stuff we just inserted.
            )))
  (rewrite [op rewriter]
    ;; here, something needs to know which rewrite mode to be in
    (-> op .getGraph Item/createNode arq/sse-zipper rewriter UpdateDrop.))

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
  (<= (reduce + (map size operations)) max-update-size))

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

(defn- graph-copyable? [repo max-update-size graph-uri]
  (let [q "
SELECT (COUNT (*) AS ?c) WHERE {
  GRAPH <%s> {
    ?s ?p ?o
  }
}"
        [{c :c}] (sparql/eager-query repo (format q graph-uri))]
    (<= c max-update-size )))

(defn- insert-data-stmt [quads]
  (UpdateDataInsert. (QuadDataAcc. (map ->jena-quad quads))))

;; TODO: What if the live graph is already managed? I think we're setting it to
;; isPublic = false

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
    {:map [graph-uri draft-graph-uri]
     :stmts [insert-data copy-graph-stmt]}))

(defn- copy-graphs-operations [draftset-id graphs-to-copy]
  (let [draftset-uri (url/->java-uri (ds/->draftset-uri draftset-id))
        now (util/get-current-time)]
    (map (partial copy-graph-operation draftset-uri now) graphs-to-copy)))

(defn- rewrite-draftset-ops [draftset-id]
  [{:stmts (-> {:draftset-uri (ds/->draftset-uri draftset-id)}
               dm/rewrite-draftset-q
               UpdateFactory/create
               .getOperations)}])

(defn- prerequisites [repo max-update-size draftset-id update-request]
  (let [affected-graphs (->> (.getOperations update-request)
                             (map affected-graphs)
                             (apply set/union))
        draftset-graphs (draftset-graphs repo draftset-id)
        graphs-to-copy (set/difference affected-graphs draftset-graphs)
        copyable? (partial graph-copyable? repo max-update-size)]
    ;; TODO: Can we do this check in the query, and have it report decent errors?
    (if-let [uncopyable-graphs (seq (remove copyable? graphs-to-copy))]
      (ex-info "Unable to copy graphs"
               {:status 500
                :headers {"Content-Type" "text/plain"}
                :body "500 Unable to copy graphs"})
      (concat
       (copy-graphs-operations draftset-id graphs-to-copy)
       (rewrite-draftset-ops draftset-id)))))

(defn- add-operations [update-request operations]
  (doseq [op operations] (.add update-request op))
  update-request)

;; get affected graphs
;; minus existing draftset graphs
;; return live->draft for request

(defn- graph-mapping [backend draftset-id update-request]
  (let [affected-graphs (->> (.getOperations update-request)
                             (map affected-graphs)
                             (apply set/union))
        request-mapping (map (fn [g] [g (dm/make-draft-graph-uri)])
                             affected-graphs)
        draftset-mapping (ops/get-draftset-graph-mapping backend draftset-id)]
    (merge request-mapping draftset-mapping)))

(defn- draftset-graph-meta-q [draftset-id]
  (let [ds-uri (ds/->draftset-uri draftset-id)]
    (str
     "SELECT ?lg ?dg (COUNT(?_s) AS ?c) WHERE { "
     (dm/with-state-graph
       "?ds <" rdf:a "> <" drafter:DraftSet "> ."
       "?dg <" drafter:inDraftSet "> ?ds ."
       "?lg <" rdf:a "> <" drafter:ManagedGraph "> ."
       "?lg <" drafter:hasDraft "> ?dg ."
       "VALUES ?ds { <" ds-uri "> }")
     "  GRAPH ?dg { ?_s ?_p ?_o } "
     "} "
     "GROUP BY ?lg ?dg ?_s")))

(defn- draftset-graph-meta [backend draftset-id]
  (let [q (draftset-graph-meta-q draftset-id)]
    (->> (sparql/eager-query backend q)
         (map (fn [{:keys [dg lg c]}]
                [lg {:size c :draft-graph-uri dg :managed? true}]))
         (into {}))))

(s/fdef draftset-graph-meta
  :args (s/cat :backend any? :draftset-id uuid?)
  :ret ::graph-meta)

(s/def ::draft-graph-uri uri?)
(s/def ::size nat-int?)
(s/def ::managed? boolean?)
(s/def ::graph-meta
  (s/map-of uri? (s/keys :req-un [::draft-graph-uri ::size ::managed?])))

(defn rewriter [graph-meta]
  (->> graph-meta
       (map (juxt key (comp :draft-graph-uri val)))
       (into {})
       (partial uri-constant-rewriter)))

(defn handler*
  [{:keys [drafter/backend max-update-size wrap-as-draftset-owner] :as opts} request]
  (let [draftset-id (:draftset-id (:params request))
        update-request (parse-body request opts)]
    (if-let [error-response (ex-data update-request)]
      error-response
      ;; TODO: UpdateRequest base-uri, prefix, etc?
      (let [prerequisites (prerequisites backend max-update-size draftset-id update-request)
            prerequisite-ops (mapcat :stmts prerequisites)
            graph-map (->> (map :map prerequisites)
                           (into (ops/get-draftset-graph-mapping backend draftset-id))
                           (map (fn [[k v]] [(str k) (str v)]))
                           (into {}))
            rewriter (partial uri-constant-rewriter graph-map)
            operations' (->> (.getOperations update-request)
                             (map #(rewrite % rewriter)))
            update-request' (-> (UpdateRequest.)
                                (add-operations prerequisite-ops)
                                (add-operations operations'))]
        ;; (println (str update-request'))
        (sparql/update! backend (str update-request'))
        {:status 204}))))

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
