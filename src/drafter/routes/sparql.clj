(ns drafter.routes.sparql
  (:require [compojure.core :refer [context defroutes routes routing let-request
                                    make-route let-routes
                                    ANY GET POST PUT DELETE HEAD]]
            [clojure.set :as set]
            [ring.util.io :as io]
            [compojure.route :refer [not-found]]
            [drafter.rdf.draft-management :as mgmt]
            [drafter.rdf.sparql-protocol :refer [sparql-end-point process-sparql-query result-handler-wrapper]]
            [drafter.rdf.sparql-rewriting :as rew]
            [taoensso.timbre :as timbre]
            [grafter.rdf.sesame :as ses]
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
      (timbre/info "select result wrapper " this  "writer: " writer)
      (solution-handler-fn writer binding-set)
      ;;(.handleSolution writer binding-set)
      )
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
      (.addBinding bs (.getName binding) (.getValue binding)))
    bs))

(defn- choose-result-rewriter [query-ast vars-in-graph-position draft->live writer]
  (if (seq vars-in-graph-position)
    ;; if there are vars in graph position - we should rewrite the
    ;; results
    (let [rewrite-graph-result (fn [^QueryResultHandler writer ^BindingSet binding-set]
                                 (timbre/debug "vars in graph position" vars-in-graph-position)

                                 (if (seq vars-in-graph-position)
                                   (let [new-binding-set (clone-binding-set binding-set)]
                                     ;; Copy binding-set as mutating it whilst writing (iterating)
                                     ;; results causes bedlam with the iteration, especially with SPARQL
                                     ;; DISTINCT queries.
                                     (doseq [var vars-in-graph-position]
                                       (when-not (.isConstant var)
                                         (let [val (.getValue binding-set (.getName var))
                                               new-uri (get draft->live val val)]
                                           (.addBinding new-binding-set (.getName var) new-uri))))
                                     (.handleSolution writer new-binding-set))
                                   (.handleSolution writer binding-set)))]
      (cond
       (instance? ParsedGraphQuery query-ast) (make-construct-result-rewriter writer draft->live)
       (instance? ParsedTupleQuery query-ast) (make-select-result-rewriter rewrite-graph-result writer)
       (instance? ParsedBooleanQuery query-ast) writer
       :else writer))

    ;; else return the standard writer
    writer))

(defn make-draft-query-rewriter [repo query-str draft-uris]
  (let [live->draft (mgmt/graph-map repo draft-uris)
        preped-query (rew/rewrite-graph-query repo query-str live->draft)]
    {:query-rewriter
     (fn [repo query-str]
       (timbre/info  "Using mapping: " live->draft)
       preped-query)

     :result-rewriter
     (fn [writer]
       (let [query-ast (-> preped-query .getParsedQuery)
             vars-in-graph-position (rew/vars-in-graph-position query-ast)
             draft->live (set/map-invert live->draft)]
         (choose-result-rewriter query-ast vars-in-graph-position draft->live writer)))
     }))

(defn- draft-query-endpoint [repo request]
  (try
    (let [{:keys [params]} request
          query-str (:query params)
          graph-uris (supplied-drafts repo request)
          {:keys [result-rewriter query-rewriter]} (make-draft-query-rewriter repo query-str graph-uris)]

      (process-sparql-query repo request
                            :query-creator-fn query-rewriter
                            :result-rewriter result-rewriter
                            :graph-restrictions graph-uris))

    (catch Exception ex
      (let [unpack #(= %1 (-> %2 ex-data :error))
            status (condp unpack ex
                     :multiple-drafts-error 412
                     nil 500)]
        {:status status :body (.getMessage ex)}))))


(defn draft-sparql-routes [mount-point repo]
  (routes
   (GET mount-point request
        (draft-query-endpoint repo request))

   (POST mount-point request
         (draft-query-endpoint repo request))))

(defn live-sparql-routes [mount-point repo]
  (sparql-end-point mount-point repo (partial mgmt/live-graphs repo)))

(defn state-sparql-routes [mount-point repo]
  (sparql-end-point mount-point repo #{mgmt/drafter-state-graph}))
