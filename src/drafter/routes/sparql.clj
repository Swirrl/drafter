(ns drafter.routes.sparql
  (:require [compojure.core :refer [context defroutes routes routing let-request
                                    make-route let-routes
                                    ANY GET POST PUT DELETE HEAD]]
            [ring.util.io :as io]
            [compojure.route :refer [not-found]]
            [compojure.handler :refer [api]])
  (:require [drafter.rdf.sesame :as store]
            [drafter.rdf.sparql-protocol :refer [sparql-end-point process-sparql-query]]
            [grafter.rdf.sesame :as ses])
  (:import [org.openrdf.query.resultio TupleQueryResultFormat BooleanQueryResultFormat]))

(defn live-sparql-routes [repo]
  (sparql-end-point "/sparql/live" repo))

(defn drafts-sparql-routes [repo]
  ;;(sparql-end-point "/sparql/drafts" repo)
  (routes
   (GET "/ring-params" request
        (prn-str request)))
  )
