(ns drafter.routes.draftsets-api
  (:require [compojure.core :refer [GET POST PUT context routes]]
            [ring.util.response :refer [redirect-after-post]]
            [drafter.responses :refer [unknown-rdf-content-type-response submit-async-job!]]
            [swirrl-server.responses :as response]
            [drafter.rdf.draftset-management :as dsmgmt]
            [drafter.backend.protocols :refer :all]
            [grafter.rdf.io :refer [mimetype->rdf-format]]))

(defn- is-quads-content-type? [rdf-format]
  (.supportsContexts rdf-format))

(defn draftset-api-routes [mount-point backend]
  (routes
   (context
    mount-point []

    (GET "/draftsets" []
         {:status 200 :headers {} :body (dsmgmt/get-all-draftsets-info backend)})

    ;;create a new draftset
    (POST "/draftset" [display-name description]
          (if (some? display-name)
            (let [draftset-id (dsmgmt/create-draftset! backend display-name description)]
              (redirect-after-post (str mount-point "/draftset/" draftset-id)))
            {:status 406 :headers {} :body "dispaly-name parameter required"}))

    (GET "/draftset/:id" [id]
         (if-let [info (dsmgmt/get-draftset-info backend (drafter.rdf.drafter-ontology/draftset-uri id))]
           {:status 200 :headers {} :body info}
           {:status 404 :headers {} :body ""}))

    (POST "/draftset/:id/data" {{draftset-id :id
                        request-content-type :content-type
                        {file-part-content-type :content-type data :tempfile} :file} :params}
          (if-let [content-type (or file-part-content-type request-content-type)]
            (let [rdf-format (mimetype->rdf-format content-type)
                  draftset-uri (drafter.rdf.drafter-ontology/draftset-uri draftset-id)]
              (cond (nil? rdf-format)
                    (unknown-rdf-content-type-response content-type)
                    
                    (is-quads-content-type? rdf-format)
                    (let [append-job (append-data-to-draftset-job backend draftset-uri data rdf-format)]
                      (submit-async-job! append-job))

                    :else (response/bad-request-response (str "Content type " content-type " does not map to an RDF format for quads"))))
            (response/bad-request-response "Content type required"))))))
