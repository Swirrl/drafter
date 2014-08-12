(ns drafter.routes.sparql
  (:require [compojure.core :refer [context defroutes routes routing let-request
                                    make-route let-routes
                                    ANY GET POST PUT DELETE HEAD]]
            [clojure.set :as set]
            [ring.util.io :as io]
            [compojure.route :refer [not-found]]
            [compojure.handler :refer [api]]
            [drafter.rdf.draft-management :as mgmt]
            [drafter.rdf.sparql-protocol :refer [sparql-end-point process-sparql-query]]
            [drafter.rdf.sparql-rewriting :as rew]
            [taoensso.timbre :as timbre]
            [grafter.rdf.sesame :as ses])
  (:import [org.openrdf.query.resultio TupleQueryResultFormat BooleanQueryResultFormat]
           [org.openrdf.query TupleQueryResultHandler BindingSet]))

(defn supplied-drafts
  "Parses out the set of \"graph\"s supplied on the request.

Returns a function that when called with a single argument (the
  database which may be ignored) will return a set of named graphs.

If no graphs are found in the request, a function that returns the set
of live graphs is returned."
  [repo request]
  (let [graphs (-> request
                  :query-params
                  (get "graph"))]
    (if graphs
      (if (instance? String graphs)
        #{graphs}
        graphs)

      (mgmt/live-graphs repo))))

(defn make-result-rewriter [solution-handler-fn writer]
  (reify TupleQueryResultHandler
    (endQueryResult [this]
      (timbre/info "endQueryResult")
      (.endQueryResult writer))
    (handleBoolean [this boolean]
      (timbre/info "handleBoolean" boolean)
      (.handleBoolean writer boolean))
    (handleLinks [this link-urls]
      (timbre/info "handleLinks" link-urls)
      (.handleLinks writer link-urls))
    (handleSolution [this binding-set]
      (timbre/info "handleSolution" binding-set)
      (solution-handler-fn writer binding-set))
    (startQueryResult [this binding-names]
      (timbre/info "startQueryResult" binding-names)
      (.startQueryResult writer binding-names))))


(defn make-draft-query-rewriter [repo query-str draft-uris]
  (let [live->draft (mgmt/graph-map repo draft-uris)
        preped-query (rew/rewrite-graph-query repo query-str live->draft)]
    {:query-rewriter
     (fn [repo query-str]
       (timbre/info  "Using mapping: " live->draft)
       preped-query)

     :result-rewriter
     (fn [writer]
       (let [binding-set (.getBindings preped-query)
             query-ast (-> preped-query .getParsedQuery)
             vars-in-graph-position (rew/vars-in-graph-position query-ast)
             draft->live (set/map-invert live->draft)]
         (if (seq vars-in-graph-position)
           (let [rewrite-graph-result (fn [^TupleQueryResultHandler writer ^BindingSet binding-set]
                                        (doseq [var vars-in-graph-position]
                                          (when-not (.isConstant var)
                                            (let [val (.getValue binding-set (.getName var))
                                                  new-uri (get draft->live val val)]
                                              (timbre/info "converting var" (.getName var) "val" val "to new-uri" new-uri)
                                              (.setBinding binding-set (.getName var) new-uri))))
                                        (timbre/info "Binding set: " binding-set )
                                        (.handleSolution writer binding-set))]
             (make-result-rewriter rewrite-graph-result writer))
           ;; else return the standard writer
           writer)))
     }))

(defn- draft-query-endpoint [repo request]
  (let [{:keys [params]} request
        query-str (:query params)
        graph-uris (supplied-drafts repo request)
        {:keys [result-rewriter query-rewriter]} (make-draft-query-rewriter repo query-str graph-uris)]

    (process-sparql-query repo request
                          :query-creator-fn query-rewriter
                          :result-rewriter result-rewriter
                          :graph-restrictions graph-uris)))


(defn draft-sparql-routes [repo]
  (routes
   (GET "/sparql/draft" request
        (draft-query-endpoint repo request))

   (POST "/sparql/draft" request
         (draft-query-endpoint repo request))))

(defn live-sparql-routes [repo]
  (sparql-end-point "/sparql/live" repo (mgmt/live-graphs repo)))

(defn state-sparql-routes [repo]
  (sparql-end-point "/sparql/state" repo #{mgmt/drafter-state-graph}))
