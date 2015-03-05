(ns drafter.routes.sparql
  (:require [compojure.core :refer [context defroutes routes routing let-request
                                    make-route let-routes
                                    ANY GET POST PUT DELETE HEAD]]
            [clojure.set :as set]
            [ring.util.io :as io]
            [compojure.route :refer [not-found]]
            [drafter.rdf.draft-management :as mgmt]
            [drafter.rdf.sparql-protocol :refer [sparql-end-point process-sparql-query result-handler-wrapper wrap-sparql-errors]]
            [drafter.rdf.sparql-rewriting :as rew]
            [clojure.tools.logging :as log]
            [grafter.rdf.repository :as ses]
            [drafter.common.sparql-routes :refer [supplied-drafts]])
  (:import [org.openrdf.query.resultio TupleQueryResultFormat BooleanQueryResultFormat]
           [org.openrdf.query QueryResultHandler TupleQueryResultHandler BindingSet Binding]
           [org.openrdf.query.parser ParsedBooleanQuery ParsedGraphQuery ParsedTupleQuery]
           [org.openrdf.query.impl MapBindingSet]
           [org.openrdf.rio RDFHandler RDFWriter]))

(defn make-select-result-rewriter
  "Creates a new SPARQLResultWriter that proxies to the supplied
  result handler, but rewrites solutions according to the supplied
  solution-handler-fn."
  [solution-handler-fn writer]
  (reify
    TupleQueryResultHandler
    (endQueryResult [this]
      (.endQueryResult writer))
    (handleBoolean [this boolean]
      (.handleBoolean writer boolean))
    (handleLinks [this link-urls]
      (.handleLinks writer link-urls))
    (handleSolution [this binding-set]
      (log/debug "select result wrapper " this  "writer: " writer)
      (solution-handler-fn writer binding-set))
    (startQueryResult [this binding-names]
      (.startQueryResult writer binding-names))))

(defn- make-construct-result-rewriter
  "Creates a result-rewriter for construct queries - not a tautology
  honest!"
  [writer draft->live]
  (if (instance? QueryResultHandler writer)
    (result-handler-wrapper writer draft->live)
    writer))

(defn clone-binding-set
  "Copy a binding set"
  [binding-set]
  (let [bs (MapBindingSet.)]
    (doseq [^Binding binding (iterator-seq (.iterator binding-set))]
      (log/debug "cloning binding: " (.getName binding) "val: "(.getValue binding))
      (.addBinding bs (.getName binding) (.getValue binding)))
    bs))

(defn- rewrite-graph-result [vars draft->live ^QueryResultHandler writer ^BindingSet binding-set]
  (let [new-binding-set (clone-binding-set binding-set)]
    ;; Copy binding-set as mutating it whilst writing (iterating)
    ;; results causes bedlam with the iteration, especially with SPARQL
    ;; DISTINCT queries.
    (log/trace "old binding set: " binding-set "new binding-set" new-binding-set)
    (doseq [var vars]
      (when-not (.isConstant var)
        (when-let [val (.getValue binding-set (.getName var))]
          ;; only rewrite results if the value is bound as a return
          ;; value (i.e. it's a named result parameter for SELECT)
          (let [new-uri (get draft->live val val)]
            (log/trace "Substituting val" val "for new-uri:" new-uri "for var:" var)
            (.addBinding new-binding-set (.getName var) new-uri)))))
    (.handleSolution writer new-binding-set)))

(defn- choose-result-rewriter [query-ast vars-in-graph-position draft->live writer]
  (cond
   (instance? ParsedGraphQuery query-ast) (make-construct-result-rewriter writer draft->live)
   (instance? ParsedTupleQuery query-ast) (let [rewriter #(rewrite-graph-result vars-in-graph-position draft->live %1 %2)]
                                            (make-select-result-rewriter rewriter writer))
   (instance? ParsedBooleanQuery query-ast) writer
   :else writer))

(defn make-draft-query-rewriter [repo query-str draft-uris]
  (let [live->draft (log/spy (mgmt/graph-map repo draft-uris))
        preped-query (rew/rewrite-graph-query repo query-str live->draft)]
    {:query-rewriter
     (fn [repo query-str]
       (log/info "Using mapping: " live->draft)
       preped-query)

     :result-rewriter
     (fn [writer]
       (let [query-ast (.getParsedQuery preped-query)
             vars-in-graph-position (rew/vars-in-graph-position query-ast)
             draft->live (set/map-invert live->draft)]
         (choose-result-rewriter query-ast vars-in-graph-position draft->live writer)))
     }))

(defn- draft-query-endpoint [repo request]
  (try
    (let [{:keys [params]} request
          query-str (:query params)
          graph-uris (log/spy :info  (supplied-drafts repo request))
          {:keys [result-rewriter query-rewriter]} (make-draft-query-rewriter repo query-str graph-uris)]

      (process-sparql-query repo request
                            :query-creator-fn query-rewriter
                            :result-rewriter result-rewriter
                            :graph-restrictions graph-uris))

    (catch clojure.lang.ExceptionInfo ex
      (let [unpack #(= %1 (-> %2 ex-data :error))
            status (condp unpack ex
                     :multiple-drafts-error 412)]
        {:status status :body (.getMessage ex)}))))

(defn draft-sparql-routes [mount-point repo]
  (wrap-sparql-errors
   (routes
    (GET mount-point request
         (draft-query-endpoint repo request))

    (POST mount-point request
          (draft-query-endpoint repo request)))))

(defn live-sparql-routes [mount-point repo]
  (sparql-end-point mount-point repo (partial mgmt/live-graphs repo)))

(defn state-sparql-routes [mount-point repo]
  (sparql-end-point mount-point repo #{mgmt/drafter-state-graph}))

(defn raw-sparql-routes [mount-point repo]
  (sparql-end-point mount-point repo))
