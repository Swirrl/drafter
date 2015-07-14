(ns drafter.routes.results-table
  (:require [compojure.core :refer [POST]]))

(defn results-table-endpoint
  [mount-path make-endpoint-f repo]
  (POST mount-path request ;[query column-names & request]
        (let [endpoint (make-endpoint-f mount-path repo)
              response (endpoint request)]
          response

          ;(-> response
          ;    (add-content-disposition ))
          )))

;; (defn dumps-endpoint
;;   "Implemented as a ring middle-ware dumps-endpoint wraps an existing
;;   ring sparql endpoint-handler and feeds it a construct query on the
;;   graph indicated by the graph-uri request parameter.

;;   As it's implemented as a middleware you can use it to provide dumps
;;   functionality on a query-rewriting endpoint, a restricted
;;   endpoint (e.g. only drafter public/live graphs) a raw endpoint, or
;;   any other."
;;   [mount-path make-endpoint-f repo]
;;   (GET mount-path request
;;        (let [{graph-uri :graph-uri} (:params request)]
;;          (if-not graph-uri
;;            {:status 500 :body "You must supply a graph-uri parameter to specify the graph to dump."}

;;            (let [construct-request (assoc-in request
;;                                              [:params :query]
;;                                              (str "CONSTRUCT { ?s ?p ?o . } WHERE { GRAPH <" graph-uri "> { ?s ?p ?o . } }"))
;;                  endpoint (make-endpoint-f mount-path repo)
;;                  response (endpoint construct-request)]

;;              (add-content-disposition response graph-uri (get-in request [:headers "accept"])))))))
