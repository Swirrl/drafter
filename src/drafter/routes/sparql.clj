(ns drafter.routes.sparql
  (:require [compojure.core :refer [context defroutes routes routing let-request
                                    make-route let-routes
                                    ANY GET POST PUT DELETE HEAD]]
            [clojure.set :as set]
            [drafter.rdf.draft-management :as mgmt]
            [drafter.rdf.sparql-protocol :refer [sparql-end-point process-sparql-query wrap-sparql-errors ->SesameSparqlExecutor
                                                 ->RewritingSesameSparqlExecutor]]
            [drafter.rdf.rewriting.result-rewriting :refer [choose-result-rewriter]]
            [drafter.rdf.rewriting.query-rewriting :refer [rewrite-sparql-string]]
            [clojure.tools.logging :as log]
            [drafter.common.sparql-routes :refer [supplied-drafts]]))

(defn- make-draft-query-rewriter
  "Build both a query rewriter and an accompanying result rewriter tied together
  in a hash-map, for supplying to our draft SPARQL endpoints as configuration."

  [live->draft]
  {:query-rewriter (fn [query] (rewrite-sparql-string live->draft query))
   :result-rewriter
   (fn [prepared-query writer]
     (let [draft->live (set/map-invert live->draft)]
       (choose-result-rewriter prepared-query draft->live writer)))})

(defn- draft-query-endpoint [repo request timeouts]
  (try
    (let [{:keys [params]} request
          graph-uris (supplied-drafts repo request)
          live->draft (log/spy(mgmt/graph-map repo graph-uris))
          {:keys [result-rewriter query-rewriter]} (make-draft-query-rewriter live->draft)
          inner-executor (->SesameSparqlExecutor repo)
          executor (->RewritingSesameSparqlExecutor inner-executor query-rewriter result-rewriter)]

      (process-sparql-query executor request
                            :graph-restrictions graph-uris
                            :query-timeouts timeouts))

    (catch clojure.lang.ExceptionInfo ex
      (let [unpack #(= %1 (-> %2 ex-data :error))
            status (condp unpack ex
                     :multiple-drafts-error 412)]
        {:status status :body (.getMessage ex)}))))

(defn draft-sparql-routes
  ([mount-point repo] (draft-sparql-routes mount-point repo nil))
  ([mount-point repo timeouts]
     (wrap-sparql-errors
      (routes
       (GET mount-point request
            (draft-query-endpoint repo request timeouts))

       (POST mount-point request
             (draft-query-endpoint repo request timeouts))))))

(defn live-sparql-routes [mount-point repo timeouts]
  (sparql-end-point mount-point repo (partial mgmt/live-graphs repo) timeouts))

(defn state-sparql-routes [mount-point repo timeouts]
  (sparql-end-point mount-point repo #{mgmt/drafter-state-graph} timeouts))

(defn raw-sparql-routes [mount-point repo timeouts]
  (sparql-end-point mount-point repo nil timeouts))
