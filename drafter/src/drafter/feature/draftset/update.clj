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
            [grafter.url :as url]
            [clojure.string :as string])
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
  (raw-operations [op opts])
  (rewrite [op rewriter]))

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

(defn- insert-data-stmt [quads]
  (UpdateDataInsert. (QuadDataAcc. (map ->jena-quad quads))))

;; TODO: What if the live graph is already managed? I think we're setting it to
;; isPublic = false

(defn- copy-graph-operation
  ;; TODO: this is the issue, we don't actually create the draft
  ;; ******
  [draftset-uri timestamp [graph-uri {:keys [draft-graph-uri]}]]
  (let [managed-graph-quads (dm/to-quads (dm/create-managed-graph graph-uri))
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
  (let [draftset-uri (url/->java-uri (ds/->draftset-uri draftset-id))
        now (util/get-current-time)]
    (mapcat (partial copy-graph-operation draftset-uri now) graphs-to-copy)))

(defn- rewrite-draftset-ops [draftset-id]
  (-> {:draftset-uri (ds/->draftset-uri draftset-id)}
      dm/rewrite-draftset-q
      UpdateFactory/create
      .getOperations))

(defn- draft-graph-quads [draftset-uri timestamp graph-uri draft-graph-uri]
  (->> (dm/create-draft-graph graph-uri draft-graph-uri timestamp draftset-uri)
       (apply dm/to-quads)))

(defn- draft-graph-stmt [draftset-uri timestamp graph-uri draft-graph-uri]
  (-> (draft-graph-quads draftset-uri timestamp graph-uri draft-graph-uri)
      (insert-data-stmt)))

(defn- manage-graph-stmt [draftset-uri timestamp graph-uri draft-graph-uri]
  (-> (concat
       (dm/to-quads (dm/create-managed-graph graph-uri))
       (draft-graph-quads draftset-uri timestamp graph-uri draft-graph-uri))
      (insert-data-stmt)))

(defn- copy-graph-stmt [graph-uri draft-graph-uri]
  (UpdateCopy. (Target/create (str graph-uri))
               (Target/create (str draft-graph-uri))))

(defn- prerequisites [graph-meta max-update-size draftset-id update-request]
  (let [draftset-uri (url/->java-uri (ds/->draftset-uri draftset-id))
        timestamp (util/get-current-time)
        graphs-to-manage (keep (fn [[lg {:keys [live? draft? draft-graph-uri]}]]
                                 (when (and (not live?) (not draft?))
                                   (manage-graph-stmt draftset-uri
                                                      timestamp
                                                      lg
                                                      draft-graph-uri)))
                               graph-meta)
        copyable? (fn [[_ {:keys [live-size]}]] (<= live-size max-update-size))
        graphs-to-copy (->> graph-meta
                            (keep (fn [[lg {:keys [live? draft? live-size draft-graph-uri]}]]
                                    (when (and live? (not draft?))
                                      (if (> live-size max-update-size)
                                        (throw
                                         (ex-info "Unable to copy graphs"
                                                  {:status 500
                                                   :headers {"Content-Type" "text/plain"}
                                                   :body "500 Unable to copy graphs"}))
                                        [(draft-graph-stmt draftset-uri
                                                           timestamp
                                                           lg
                                                           draft-graph-uri)
                                         (copy-graph-stmt lg draft-graph-uri)]))))
                            (apply concat))]
    (concat graphs-to-manage
            graphs-to-copy
            (if (seq graphs-to-copy) (rewrite-draftset-ops draftset-id) []))))

(defn- data-insert-delete-ops
  [op {:keys [max-update-size rewriter draftset-id graph-meta] :as opts}]
  ;; do we /need/ to copy?
  ;; (println 'op (str op))
  (concat (prerequisites graph-meta max-update-size draftset-id op)
          [(rewrite op rewriter)]))

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

(defn- add-operations [update-request operations]
  (doseq [op operations] (.add update-request op))
  update-request)

(defn- graph-meta-q [draftset-id live-graphs]
  (let [ds-uri (ds/->draftset-uri draftset-id)
        live-values-str (string/join " " (map #(str "<" % ">") live-graphs))]
    (str "
SELECT ?lg ?dg (COUNT(?s1) AS ?c1) (COUNT(?s2) AS ?c2) WHERE {
  { GRAPH ?dg { ?s1 ?p1 ?o1 }
    GRAPH <http://publishmydata.com/graphs/drafter/drafts> {
      ?lg a <http://publishmydata.com/def/drafter/ManagedGraph> .
      ?ds <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://publishmydata.com/def/drafter/DraftSet> .
      ?dg <http://publishmydata.com/def/drafter/inDraftSet> ?ds .
      ?lg <http://publishmydata.com/def/drafter/hasDraft> ?dg .
    }
    VALUES ?ds { <" ds-uri "> }
  } UNION {
    GRAPH ?lg { ?s2 ?p2 ?o2 }
    GRAPH <http://publishmydata.com/graphs/drafter/drafts> {
      ?lg a <http://publishmydata.com/def/drafter/ManagedGraph> .
      MINUS {
        ?lg <http://publishmydata.com/def/drafter/hasDraft> ?_dg .
      }
    }
    VALUES ?lg { " live-values-str " }
  }
}
GROUP BY ?lg ?dg")))

(defn- graph-meta [backend draftset-id update-request]
  (let [new-graph (fn [lg]
                    [lg {:draft-graph-uri (dm/make-draft-graph-uri)
                         :draft? false
                         :live? false
                         :draft-size 0
                         :live-size 0}])
        affected-graphs (->> (.getOperations update-request)
                             (map affected-graphs)
                             (apply set/union))]
    (->> (graph-meta-q draftset-id affected-graphs)
         (sparql/eager-query backend)
         (filter (fn [{:keys [dg lg]}] (or dg lg)))
         (map (fn [{:keys [dg lg c1 c2]}]
                [lg {:draft-graph-uri (or dg (dm/make-draft-graph-uri))
                     :draft? (boolean dg)
                     :live? (boolean lg)
                     :draft-size c1
                     :live-size c2}]))
         (into {})
         (merge (->> affected-graphs (map new-graph) (into {}))))))

(s/def ::draft-graph-uri uri?)
(s/def ::draft? boolean?)
(s/def ::live? boolean?)
(s/def ::draft-size nat-int?)
(s/def ::live-size nat-int?)
(s/def ::graph-meta
  (s/map-of uri? (s/keys :req-un [::draft-graph-uri
                                  ::draft?
                                  ::live?
                                  ::draft-size
                                  ::live-size])))

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
  (raw-operations [op opts]
    (data-insert-delete-ops op opts))
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
  (raw-operations [op opts]
    (data-insert-delete-ops op opts))
  (rewrite [op rewriter]
    (->> (.getQuads op)
         (map (partial rewrite-quad rewriter))
         (QuadDataAcc.)
         (UpdateDataInsert.)))

  UpdateDrop
  (affected-graphs [op]
;(URI. (.getURI (.getGraph op)))
    #{})
  (size [op] 1)
  (raw-operations [op {:keys [rewriter draftset-uri graph-meta]}]
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
    ;; what does ^^ this mean now? still relevant?
    (-> op .getGraph Item/createNode arq/sse-zipper rewriter UpdateDrop.)))

(defn- rewriter [graph-meta]
  (->> graph-meta
       (map (juxt (comp str key) (comp str :draft-graph-uri val)))
       (into {})
       (partial uri-constant-rewriter)))

(defn handler*
  [{:keys [drafter/backend max-update-size] :as opts} request]
  (let [draftset-id (:draftset-id (:params request))
        update-request (parse-body request opts)]
    (if-let [error-response (ex-data update-request)]
      error-response
      ;; TODO: UpdateRequest base-uri, prefix, etc?
      (let [graph-meta (graph-meta backend draftset-id update-request)
            opts {:graph-meta graph-meta
                  :rewriter (rewriter graph-meta)
                  :draftset-id draftset-id
                  :max-update-size max-update-size}
            update-request' (->> (.getOperations update-request)
                                 (mapcat #(raw-operations % opts))
                                 (add-operations (UpdateRequest.)))]
        (clojure.pprint/pprint graph-meta)
        ;; (println (str update-request'))
        (sparql/update! backend (str update-request'))
        {:status 204}))))

(defn handler
  [{:keys [wrap-as-draftset-owner] :as opts}]
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
