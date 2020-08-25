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

;; TODO: sparql protocol handler for update
;; TODO: which graphs in data?
;; TODO: update graph metadata
;; TODO: copy-graph
;; TODO: write-lock?

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

(defn- ->jena-quad [{:keys [c s p o]}]
  (letfn [(->node [x]
            (if (uri? x)
              (NodeFactory/createURI (str x))
              (NodeFactory/createLiteral (str x))))]
    (let [[c s p o] (map ->node [c s p o])]
      (Quad. c s p o))))

(defprotocol UpdateOperation
  (affected-graphs [op])
  (size [op])
  (raw-operations [op opts])
  (rewrite [op rewriter]))

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

(defn- graphs-to-manage [draftset-uri timestamp graph-meta]
  (keep (fn [[lg {:keys [live? draft? draft-graph-uri]}]]
          (when (and (not live?) (not draft?))
            (manage-graph-stmt draftset-uri timestamp lg draft-graph-uri)))
        graph-meta))

(defn- within-limit? [operations {:keys [max-update-size] :as opts}]
  (<= (reduce + (map size operations)) max-update-size))

(defn- processable? [operation]
  (satisfies? UpdateOperation operation))

(defn- just-in-live? [g meta]
  (let [{:keys [live? draft?]} (get meta g)]
    (and live? (not draft?))))

(defn- in-draftset? [g meta]
  (let [{:keys [live? draft?]} (get meta g)]
    draft?))

(defn- prior-reference? [g op ops]
  (not (empty? (take-while (complement #{op}) ops))))

(defn- copy? [g op {:keys [live? draft? live-size draft-graph-uri ops]}]
  (and live?
       (not draft?)
       (not (prior-reference? g op ops))))

(defn- graphs-to-copy [op draftset-uri timestamp max-update-size graph-meta]
  (->> graph-meta
       (keep (fn [[lg {:keys [live? draft? live-size draft-graph-uri] :as opts}]]
               (when (copy? lg op opts)
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
       (apply concat)))

(defn- data-insert-delete-ops
  [op {:keys [max-update-size rewriter draftset-id graph-meta] :as opts}]
  (let [draftset-uri (url/->java-uri (ds/->draftset-uri draftset-id))
        timestamp (util/get-current-time)
        manage (graphs-to-manage draftset-uri timestamp graph-meta)
        copy (graphs-to-copy op draftset-uri timestamp max-update-size graph-meta)
        rewrite-ds (when (seq copy) (rewrite-draftset-ops draftset-id))
        op [(rewrite op rewriter)]]
    (concat manage copy rewrite-ds op)))

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

(defn- get-graph-meta [backend draftset-id update-request]
  (let [new-graph (fn [lg]
                    [lg {:draft-graph-uri (dm/make-draft-graph-uri)
                         :draft? false
                         :live? false
                         :draft-size 0
                         :live-size 0}])
        affected-graphs (->> (.getOperations update-request)
                             (map (juxt identity affected-graphs)))
        graph-op-order (reduce (fn [acc [op gs]]
                                 (reduce (fn [acc g]
                                           (if-let [ops (acc g)]
                                             (update acc g conj op)
                                             (assoc acc g [op])))
                                         acc
                                         gs))
                               {}
                               affected-graphs)
        affected-graphs (->> (map second affected-graphs)
                             (apply set/union)
                             (map new-graph)
                             (map (fn [[lg m]]
                                    [lg (assoc m :ops (graph-op-order lg))]))
                             (into {}))]
    (->> (graph-meta-q draftset-id (keys affected-graphs))
         (sparql/eager-query backend)
         (filter (fn [{:keys [dg lg]}] (or dg lg)))
         (map (fn [{:keys [dg lg c1 c2]}]
                [lg {:draft-graph-uri (or dg (dm/make-draft-graph-uri))
                     :draft? (boolean dg)
                     :live? (boolean lg)
                     :draft-size c1
                     :live-size c2}]))
         (into {})
         (merge-with merge affected-graphs))))

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
    #{(URI. (.getURI (.getGraph op)))})
  (size [op] 1)
  (raw-operations [op {:keys [rewriter draftset-id graph-meta max-update-size]}]
    (let [g (URI. (.getURI (.getGraph op)))
          {:keys [live? draft? draft-graph-uri draft-size ops]} (graph-meta g)
          draftset-uri (url/->java-uri (ds/->draftset-uri draftset-id))]
      (cond (just-in-live? g graph-meta)
            [(manage-graph-stmt draftset-uri
                                (util/get-current-time)
                                g
                                draft-graph-uri)]
            (in-draftset? g graph-meta)
            ;; There are 2 cases here:
            ;; a) The graph is in live (and has a draft)
            ;; b) The graph is not in live (but has a draft)
            ;; Do we do the same thing in both cases? I think we want to remove
            ;; all the triples, I.E., DROP GRAPH g; Then, the draft graph will
            ;; be deleted, and so will the live if present.
            (if (<= draft-size max-update-size)
              [(rewrite op rewriter)]
              (throw (ex-info "Unable to copy graphs"
                              {:status 500
                               :headers {"Content-Type" "text/plain"}
                               :body "500 Unable to copy graphs"})))
            (and (not live?)
                 (not draft?)
                 (prior-reference? g op ops))
            [(rewrite op rewriter)]
            (.isSilent op)
            [] ;; NOOP
            :not-silent
            (throw (ex-info (str "Source graph " g
                                 " does not exist, cannot proceed with update.")
                            {:type :error}))))) ;; NOOP
  (rewrite [op rewriter]
    ;; here, something needs to know which rewrite mode to be in
    ;; what does ^^ this mean now? still relevant?
    (let [node (-> op .getGraph Item/createNode arq/sse-zipper rewriter)]
      (UpdateDrop. node (.isSilent op)))))

(defn- rewriter [graph-meta]
  (->> graph-meta
       (map (juxt (comp str key) (comp str :draft-graph-uri val)))
       (into {})
       (partial uri-constant-rewriter)))

(defn- handler*
  [{:keys [drafter/backend max-update-size] :as opts} request]
  (let [draftset-id (:draftset-id (:params request))
        update-request (parse-body request opts)]
    (if-let [error-response (ex-data update-request)]
      error-response
      ;; TODO: UpdateRequest base-uri, prefix, etc?
      (let [graph-meta (get-graph-meta backend draftset-id update-request)
            opts {:graph-meta graph-meta
                  :rewriter (rewriter graph-meta)
                  :draftset-id draftset-id
                  :max-update-size max-update-size}
            update-request' (->> (.getOperations update-request)
                                 (mapcat #(raw-operations % opts))
                                 (add-operations (UpdateRequest.)))]
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
