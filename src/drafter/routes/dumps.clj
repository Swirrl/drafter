(ns drafter.routes.dumps
  (:require [ring.util.io :as rio]
            [clojure.string :as str]
            [grafter.rdf.repository :as repo]
            [drafter.rdf.sparql-protocol :as sparql]
            [compojure.core :refer [GET]]
            [clojure.tools.logging :as log]
            [ring.middleware.accept :refer [wrap-accept]]))

(defn dumps-route
  ([mount-path repo]
   (dumps-route mount-path repo nil))

  ([mount-path repo restrictions]
   (GET mount-path {{graph-uri "graph-uri"} :params
                    :as request}
        (if-not graph-uri
          {:status 500 :body "You must supply a graph-uri parameter to specify the graph to dump."}

          (let [construct-request (assoc-in request [:params :query] (str "CONSTRUCT { ?s ?p ?o . } WHERE { GRAPH <" graph-uri "> { ?s ?p ?o . } }"))]
            (println construct-request)
            (sparql/process-sparql-query repo construct-request
                                         :graph-restrictions restrictions))))))
