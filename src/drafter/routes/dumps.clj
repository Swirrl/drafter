(ns drafter.routes.dumps
  (:require [compojure.core :refer [GET]])
  (:import (org.openrdf.rio RDFFormat)))

(defn graph-slug [s]
  (last (clojure.string/split s #"/")))

(defn safe-filename [s]
  (.replaceAll s "[^a-z,A-Z,0-9]" "-"))

(defn add-file-extension [f accept]
  (str f (if-let [format (RDFFormat/forMIMEType accept) ]
           (str "." (.getDefaultFileExtension format))
           ".nt")))

(defn make-filename [graph-uri mimetype]
  (-> graph-uri
      graph-slug
      safe-filename
      (add-file-extension mimetype)))

(defn add-content-disposition [response graph-uri mimetype]
  (let [filename (make-filename graph-uri mimetype)]
    (assoc-in response [:headers "Content-Disposition"]
              (str "attachment; filename=\"" filename "\""))))

(defn dumps-endpoint
  "Implemented as a ring middle-ware dumps-endpoint wraps an existing
  ring sparql endpoint-handler and feeds it a construct query on the
  graph indicated by the graph-uri request parameter.

  As it's implemented as a middleware you can use it to provide dumps
  functionality on a query-rewriting endpoint, a restricted
  endpoint (e.g. only drafter public/live graphs) a raw endpoint, or
  any other."
  [mount-path make-endpoint-f repo]
  (GET mount-path request
       (let [{graph-uri :graph-uri} (:params request)]
         (if-not graph-uri
           {:status 500 :body "You must supply a graph-uri parameter to specify the graph to dump."}

           (let [construct-request (assoc-in request
                                             [:params :query]
                                             (str "CONSTRUCT { ?s ?p ?o . } WHERE { GRAPH <" graph-uri "> { ?s ?p ?o . } }"))
                 endpoint (make-endpoint-f mount-path repo)
                 response (endpoint construct-request)]

             (add-content-disposition response graph-uri (get-in request [:headers "accept"])))))))
