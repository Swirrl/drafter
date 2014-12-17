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
   (GET mount-path {{graph-uri "graph-uri"} :query-params
                    :as request }
        (let [construct-request (assoc-in request [:params :query] (str "CONSTRUCT { ?s ?p ?o . } WHERE { GRAPH <" graph-uri "> { ?s ?p ?o . } }"))]
          (sparql/process-sparql-query repo construct-request
                                       :graph-restrictions restrictions)))))
