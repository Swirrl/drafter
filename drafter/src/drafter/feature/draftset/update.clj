(ns drafter.feature.draftset.update
  (:require
   [clojure.set :as set]
   [clojure.spec.alpha :as s]
   [clojure.string :as string]
   [drafter.backend.draftset.arq :as arq]
   [drafter.backend.draftset.draft-management :as dm]
   [drafter.backend.draftset.graphs :as graphs]
   [drafter.backend.draftset.rewrite-query :refer [uri-constant-rewriter]]
   [drafter.draftset :as ds]
   [drafter.errors :refer [wrap-encode-errors]]
   [drafter.feature.draftset-data.common :refer [touch-graph-in-draftset]]
   [drafter.rdf.drafter-ontology :refer :all]
   [drafter.rdf.jena :as jena]
   [drafter.rdf.sparql :as sparql]
   [drafter.time :as time]
   [drafter.write-scheduler :refer [with-lock]]
   [grafter-2.rdf4j.repository :as repo]
   [grafter.url :as url]
   [grafter.vocabularies.rdf :refer :all]
   [integrant.core :as ig]
   [ring.middleware.cors :as cors]
   [ring.util.request :as request])
  (:import java.net.URI
           [org.apache.jena.sparql.algebra Algebra OpAsQuery]
           [org.apache.jena.sparql.modify.request
            QuadDataAcc Target
            UpdateCopy UpdateDrop UpdateDataDelete UpdateDataInsert]
           org.apache.jena.sparql.sse.SSE
           org.apache.jena.sparql.sse.Item
           [org.apache.jena.query Syntax]
           [org.apache.jena.update UpdateFactory]))

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
  (raw-operations [op opts])
  (rewrite [op rewriter]))

(defn- forbidden-request [& [msg]]
  (ex-info (str "403 Forbidden" (when msg (str ": " msg))) {:error :forbidden}))

(defn- unprocessable-request [unprocessable]
  (ex-info "422 Unprocessable Entity" {:error :unprocessable-request}))

(defn- payload-too-large [operations]
  (ex-info "413 Payload Too Large." {:error :payload-too-large}))

(defn- rewrite-draftset-ops [draftset-id]
  (-> {:draftset-uri (ds/->draftset-uri draftset-id)}
      dm/rewrite-draftset-q
      UpdateFactory/create
      .getOperations))

(defn- draft-graph-stmt [graph-manager draftset-uri timestamp graph-uri draft-graph-uri]
  (-> (graphs/new-draft-user-graph-statements graph-manager graph-uri draft-graph-uri timestamp draftset-uri)
      (jena/insert-data-stmt)))

(defn- manage-graph-stmt [graph-manager draftset-uri timestamp graph-uri draft-graph-uri]
  (-> (concat
        (graphs/new-managed-user-graph-statements graph-manager graph-uri)
        (graphs/new-draft-user-graph-statements graph-manager graph-uri draft-graph-uri timestamp draftset-uri))
      (jena/insert-data-stmt)))

(defn- copy-graph-stmt [graph-uri draft-graph-uri]
  (UpdateCopy. (Target/create (str graph-uri))
               (Target/create (str draft-graph-uri))))

(defn- graphs-to-manage [graph-manager draftset-uri timestamp graph-meta]
  (keep (fn [[lg {:keys [live? draft? draft-graph-uri]}]]
          (when (and (not live?) (not draft?))
            (manage-graph-stmt graph-manager draftset-uri timestamp lg draft-graph-uri)))
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

(defn- copy? [g op {:keys [live? draft? live-size draft-graph-uri ops]}]
  (and live?
       (not draft?)
       (not (prior-reference? g op ops))))

(defn- graphs-to-copy [graph-manager op draftset-uri timestamp max-update-size graph-meta]
  (->> graph-meta
       (keep (fn [[lg {:keys [live? draft? live-size draft-graph-uri] :as opts}]]
               (when (copy? lg op opts)
                 (if (> live-size max-update-size)
                   (throw (ex-info "Unable to copy graphs" {:type :error}))
                   [(draft-graph-stmt graph-manager
                                      draftset-uri
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
  [op {:keys [graph-manager clock max-update-size rewriter draftset-id graph-meta] :as opts}]
  (let [draftset-uri (url/->java-uri (ds/->draftset-uri draftset-id))
        timestamp (time/now clock)
        manage (graphs-to-manage graph-manager draftset-uri timestamp graph-meta)
        copy (graphs-to-copy graph-manager op draftset-uri timestamp max-update-size graph-meta)
        touch (graphs-to-touch op draftset-uri timestamp graph-meta)
        rewrite-ds (when (seq copy) (rewrite-draftset-ops draftset-id))
        op [(rewrite op rewriter)]]
    (concat manage copy rewrite-ds touch op)))

(defn- graph-meta-q [draftset-id live-graphs]
  (let [ds-uri (ds/->draftset-uri draftset-id)
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
  (raw-operations [op {:keys [rewriter graph-manager clock draftset-id graph-meta max-update-size]}]
    (let [g (URI. (.getURI (.getGraph op)))
          {:keys [live? draft? draft-graph-uri draft-size ops]} (graph-meta g)
          draftset-uri (url/->java-uri (ds/->draftset-uri draftset-id))
          now (time/now clock)
          noop []]
      (cond (just-in-live? g graph-meta)
            [(manage-graph-stmt graph-manager draftset-uri now g draft-graph-uri)]
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

(defn- update! [global-writes-lock
                backend
                graph-manager
                clock
                max-update-size
                draftset-id
                update-request]
  ;; We have to take a global lock here because we read from the database to
  ;; determine how to rewrite the query, so if the data changes underneath us
  ;; the rewriting is invalid.
  (with-lock global-writes-lock :update
    (let [graph-meta (get-graph-meta backend draftset-id update-request)
          rewriter-map (rewriter-map graph-meta)
          rewriter (partial uri-constant-rewriter rewriter-map)
          opts {:graph-meta graph-meta
                :rewriter rewriter
                :draftset-id draftset-id
                :graph-manager graph-manager
                :clock clock
                :max-update-size max-update-size}
          update-request' (->> (.getOperations update-request)
                               (mapcat #(raw-operations % opts))
                               (jena/->update-string))]
      (sparql/update! backend update-request'))))

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

(defn- protected? [{:keys [drafter.backend.draftset.graphs/manager] :as opts} op]
  (some (partial graphs/protected-graph? manager) (affected-graphs op)))

(defn- parse-update-param [opts {:keys [update from]} max-update-size]
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
    (cond (some (partial protected? opts) operations)
          (throw (forbidden-request "Protected graphs in update request"))
          (seq unprocessable)
          (throw (unprocessable-request unprocessable))
          (not (within-limit? operations max-update-size))
          (throw (payload-too-large operations))
          :else
          update-request)))

(defn- prepare-update [backend request]
  ;; NOTE: Currently unused. None of the Update types that we have implemented
  ;; support `using-(named-)graph-uri`.
  (with-open [repo (repo/->connection backend)]
    (let [params (parse-update-params request)
          default-graph (:using-graph-uri params)
          named-graphs (:using-named-graph-uri params)
          dataset (repo/make-restricted-dataset :default-graph default-graph
                                                :named-graphs named-graphs)]
      (doto (.prepareUpdate repo (:update params))
        (.setDataset dataset)))))

(defn- parse-update [opts request max-update-size]
  (let [{:keys [request-method body query-params form-params]} request]
    (if (= request-method :post)
      (let [draftset-id (parse-draftset-id request)
            params (parse-update-params request)
            update-request (parse-update-param opts params max-update-size)]
        (assoc params :update-request update-request :draftset-id draftset-id))
      (throw (ex-info "Method not supported"
                      {:error :method-not-allowed :method request-method})))))

(defn- handler*
  [{:keys [drafter/backend
           drafter.backend.draftset.graphs/manager
           max-update-size
           :drafter/global-writes-lock
           ::time/clock]
    :as opts}
   request]
  (let [{:keys [update-request draftset-id]}
        (parse-update opts request max-update-size)]
    (update! global-writes-lock
             backend
             manager
             clock
             max-update-size
             draftset-id
             update-request)
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
