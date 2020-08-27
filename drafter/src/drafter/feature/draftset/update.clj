(ns drafter.feature.draftset.update
  (:require [clojure.set :as set]
            [clojure.spec.alpha :as s]
            [drafter.backend.draftset.arq :as arq]
            [drafter.backend.draftset.draft-management :as dm]
            [drafter.backend.draftset.operations :as ops]
            [drafter.backend.draftset.rewrite-query :refer [uri-constant-rewriter]]
            [drafter.feature.draftset-data.common :refer [touch-graph-in-draftset]]
            [drafter.rdf.drafter-ontology :refer :all]
            [drafter.draftset :as ds]
            [drafter.rdf.sparql :as sparql]
            [drafter.util :as util]
            [integrant.core :as ig]
            [ring.middleware.cors :as cors]
            [swirrl-server.errors :refer [wrap-encode-errors]]
            [grafter.vocabularies.rdf :refer :all]
            [grafter.url :as url]
            [clojure.string :as string]
            [ring.util.request :as request]
            [grafter-2.rdf4j.repository :as repo])
  (:import java.net.URI
           java.util.UUID
           org.apache.jena.graph.NodeFactory
           [org.apache.jena.sparql.algebra Algebra OpAsQuery]
           org.apache.jena.sparql.core.Quad
           [org.apache.jena.sparql.modify.request
            QuadDataAcc Target
            UpdateCopy UpdateDrop UpdateDataDelete UpdateDeleteInsert UpdateDataInsert]
           org.apache.jena.sparql.sse.SSE
           org.apache.jena.sparql.sse.Item
           [org.apache.jena.query Syntax]
           [org.apache.jena.update UpdateFactory UpdateRequest]
           java.time.OffsetDateTime
           org.apache.jena.datatypes.xsd.XSDDatatype))

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

(defn- ->literal [x]
  (let [t (condp = (type x)
            OffsetDateTime XSDDatatype/XSDdateTime
            nil)]
    (NodeFactory/createLiteral (str x) t)))

(defn- ->jena-quad [{:keys [c s p o]}]
  (letfn [(->node [x]
            (if (uri? x)
              (NodeFactory/createURI (str x))
              (->literal x)))]
    (let [[c s p o] (map ->node [c s p o])]
      (Quad. c s p o))))

(defprotocol UpdateOperation
  (affected-graphs [op])
  (size [op])
  (raw-operations [op opts])
  (rewrite [op rewriter]))

(defn- unprocessable-request [unprocessable]
  (ex-info "422 Unprocessable Entity" {:error :unprocessable-request}))

(defn- payload-too-large [operations]
  (ex-info "413 Payload Too Large." {:error :payload-too-large}))

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

(defn- within-limit? [operations max-update-size]
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

(defn- copy-allowed? [g graph-meta]
  true
  )

(defn- copy? [g op {:keys [live? draft? live-size draft-graph-uri ops]}]
  (and live?
       (not draft?)
       (copy-allowed? g {})
       (not (prior-reference? g op ops))))

(defn- graphs-to-copy [op draftset-uri timestamp max-update-size graph-meta]
  (->> graph-meta
       (keep (fn [[lg {:keys [live? draft? live-size draft-graph-uri] :as opts}]]
               (when (copy? lg op opts)
                 (if (> live-size max-update-size)
                   (throw (ex-info "Unable to copy graphs" {:type :error}))
                   [(draft-graph-stmt draftset-uri
                                      timestamp
                                      lg
                                      draft-graph-uri)
                    (copy-graph-stmt lg draft-graph-uri)]))))
       (apply concat)))

(defn- graphs-to-touch [op draftset-uri timestamp graph-meta]
  (keep (fn [[lg {:keys [live? draft? draft-graph-uri]}]]
          (when (and live? draft?)
            (touch-graph-in-draftset draftset-uri draft-graph-uri timestamp)))
        graph-meta))

(defn- data-insert-delete-ops
  [op {:keys [max-update-size rewriter draftset-id graph-meta] :as opts}]
  (let [draftset-uri (url/->java-uri (ds/->draftset-uri draftset-id))
        timestamp (util/get-current-time)
        manage (graphs-to-manage draftset-uri timestamp graph-meta)
        copy (graphs-to-copy op draftset-uri timestamp max-update-size graph-meta)
        touch (graphs-to-touch op draftset-uri timestamp graph-meta)
        rewrite-ds (when (seq copy) (rewrite-draftset-ops draftset-id))
        op [(rewrite op rewriter)]]
    (concat manage copy rewrite-ds touch op)))

(defn- parse-body [request opts]
  (let [update-request (-> request :body UpdateFactory/create)
        operations (.getOperations update-request)
        unprocessable (remove processable? operations)]
    (cond (seq unprocessable)
          (throw (unprocessable-request unprocessable))
          (not (within-limit? operations opts))
          (throw (payload-too-large operations))
          :else
          update-request)))

(defn- add-operations [update-request operations]
  (doseq [op operations] (.add update-request op))
  update-request)

(defn- graph-meta-q [draftset-id live-graphs]
  (let [ds-uri (ds/->draftset-uri draftset-id)
        live-values-str (string/join " " (map #(str "<" % ">") live-graphs))
        live-only-values (string/join " " (map #(str "( <" % "> <" ds-uri "> )") live-graphs))]
    ;; TODO: Surely there's a better query than this to do the same? At least
    ;; the first two?
    (str "
SELECT ?lg ?dg (COUNT(DISTINCT ?s1) AS ?c1) (COUNT(DISTINCT ?s2) AS ?c2) WHERE {
  { GRAPH ?dg { ?s1 ?p1 ?o1 }
    GRAPH ?lg { ?s2 ?p2 ?o2 }
    GRAPH <http://publishmydata.com/graphs/drafter/drafts> {
      ?lg a <http://publishmydata.com/def/drafter/ManagedGraph> .
      ?ds <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://publishmydata.com/def/drafter/DraftSet> .
      ?dg <http://publishmydata.com/def/drafter/inDraftSet> ?ds .
      ?lg <http://publishmydata.com/def/drafter/hasDraft> ?dg .
    }
    VALUES ?ds { <" ds-uri "> }
  } UNION {
    GRAPH ?dg { ?s1 ?p1 ?o1 }
    FILTER NOT EXISTS { GRAPH ?lg { ?s2 ?p2 ?o2 } }
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
      ?ds <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://publishmydata.com/def/drafter/DraftSet> .
      FILTER NOT EXISTS {
        ?dg <http://publishmydata.com/def/drafter/inDraftSet> ?ds .
        ?lg <http://publishmydata.com/def/drafter/hasDraft> ?dg .
      }
    }
    VALUES ( ?lg ?ds ) { " live-only-values " }
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
                             (into {}))
        db-graphs (->> (graph-meta-q draftset-id (keys affected-graphs))
                       (sparql/eager-query backend))]
    (->> db-graphs
         (filter (fn [{:keys [dg lg]}] (or dg lg)))
         (map (fn [{:keys [dg lg c1 c2]}]
                [lg {:draft-graph-uri (or dg (dm/make-draft-graph-uri))
                     :draft? (boolean dg)
                     :live? (boolean lg)
                     :draft-size c1
                     :live-size c2}]))
         (into {})
         (merge-with merge affected-graphs))))


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
          draftset-uri (url/->java-uri (ds/->draftset-uri draftset-id))
          now (util/get-current-time)
          noop []]
      (cond (just-in-live? g graph-meta)
            [(manage-graph-stmt draftset-uri now g draft-graph-uri)]
            (in-draftset? g graph-meta)
            (if (<= draft-size max-update-size)
              [(touch-graph-in-draftset draftset-uri draft-graph-uri now)
               (rewrite op rewriter)]
              (throw (ex-info "Unable to copy graphs" {:type :error})))
            (and (not live?)
                 (not draft?)
                 (prior-reference? g op ops))
            [(rewrite op rewriter)]
            (.isSilent op)
            noop
            :not-silent
            (throw (ex-info (str "Source graph " g
                                 " does not exist, cannot proceed with update.")
                            {:type :error})))))
  (rewrite [op rewriter]
    (let [node (-> op .getGraph Item/createNode arq/sse-zipper rewriter)]
      (UpdateDrop. node (.isSilent op)))))

(defn- rewriter-map [graph-meta]
  (->> graph-meta
       (map (juxt (comp str key) (comp str :draft-graph-uri val)))
       (into {})))

(defn update! [backend max-update-size draftset-id update-request]
  (let [graph-meta (get-graph-meta backend draftset-id update-request)
        rewriter-map (rewriter-map graph-meta)
        rewriter (partial uri-constant-rewriter rewriter-map)
        opts {:graph-meta graph-meta
              :rewriter rewriter
              :draftset-id draftset-id
              :max-update-size max-update-size}
        update-request' (->> (.getOperations update-request)
                             (mapcat #(raw-operations % opts))
                             (add-operations (UpdateRequest.)))]
    ;; (println (str update-request'))
    (sparql/update! backend (str update-request'))))


;; Handler

(defn- parse-update-params [{:keys [body query-params form-params] :as request}]
  (case (request/content-type request)
    "application/x-www-form-urlencoded"
    {:update (get form-params "update")
     :using-graph-uri (get form-params "using-graph-uri")
     :using-named-graph-uri (get form-params "using-named-graph-uri")
     :from "'update' form parameter"}
    "application/sparql-update"
    {:update body
     :using-graph-uri (get query-params "using-graph-uri")
     :using-named-graph-uri (get query-params "using-named-graph-uri")
     :from "body"}
    (throw (ex-info "Bad request" {:error :bad-request}))))

(defn- parse-draftset-id [request]
  (or (some-> request :route-params :id)
      (throw (ex-info "Parameter draftset-id must be provided"
                      {:error :bad-request}))))

(defn- parse-update-param [{:keys [update from]} max-update-size]
  (let [update' (cond (string? update)
                      update
                      (instance? java.io.InputStream update)
                      (slurp update)
                      (coll? update)
                      (throw (ex-info "Exactly one query parameter required"
                                      {:error :unprocessable-request}))
                      :else
                      (throw (ex-info (str "Expected SPARQL query in " from)
                                      {:error :unprocessable-request})))
        update-request (UpdateFactory/create update' Syntax/syntaxSPARQL_11)
        operations (.getOperations update-request)
        unprocessable (remove processable? operations)]
    (cond (seq unprocessable)
          (throw (unprocessable-request unprocessable))
          (not (within-limit? operations max-update-size))
          (throw (payload-too-large operations))
          :else
          update-request)))

;; TODO: First apply restricted dataset?
;; TODO: Create prepareUpdate ?
;; TODO: Convert to jena UpdateRequest?
;; TODO: rewrite from restricted Update?
(defn- parse-update [request max-update-size]
  (let [{:keys [request-method body query-params form-params]} request]
    (if (= request-method :post)
      (let [draftset-id (parse-draftset-id request)
            params (parse-update-params request)
            update-request (parse-update-param params max-update-size)]
        (assoc params :update-request update-request :draftset-id draftset-id))
      (throw (ex-info "Method not supported"
                      {:error :method-not-allowed :method request-method})))))

(defn- prepare-update [backend request]
  (with-open [repo (repo/->connection backend)]
    (let [params (parse-update-params request)
          default-graph (:using-graph-uri params)
          named-graphs (:using-named-graph-uri params)
          dataset (repo/make-restricted-dataset :default-graph default-graph
                                                :named-graphs named-graphs)]
      (doto (.prepareUpdate repo (:update params))
        (.setDataset dataset)))))

(defn- handler*
  [{:keys [drafter/backend max-update-size] :as opts} request]
  ;; TODO: handle SPARQL protocol
  ;; TODO: sparql protocol handler for update
  ;; TODO: write-lock?
  (let [{:keys [update-request draftset-id]} (parse-update request max-update-size)]
    (update! backend max-update-size draftset-id update-request)
    {:status 204}))

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
