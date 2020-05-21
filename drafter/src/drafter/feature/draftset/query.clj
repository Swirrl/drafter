(ns drafter.feature.draftset.query
  (:require [clojure.spec.alpha :as s]
            [drafter.backend :as backend]
            [drafter.routes.draftsets-api :refer [parse-union-with-live-handler]]
            [drafter.rdf.sparql-protocol :as sp :refer [sparql-protocol-handler]]
            [ring.middleware.cors :as cors]
            [integrant.core :as ig]))

(defn handler
  [{backend :drafter/backend :keys [wrap-as-draftset-owner timeout-fn]}]
  (wrap-as-draftset-owner
   (parse-union-with-live-handler
    (fn [{{:keys [draftset-id union-with-live]} :params :as request}]
      (let [executor (backend/endpoint-repo backend draftset-id {:union-with-live? union-with-live})
            handler (sparql-protocol-handler {:repo executor :timeout-fn timeout-fn})]
        (handler request))))))

(defmethod ig/pre-init-spec ::handler [_]
  (s/keys :req [:drafter/backend]
          :req-un [::wrap-as-draftset-owner ::sp/timeout-fn]))

(def cors-allowed-headers
  #{"Accept-Encoding" "Authorization" "authorization" "DNT" "Accept"
    "Cache-Control" "Content-Type" "content-type" "If-Modified-Since"
    "Keep-Alive" "User-Agent" "X-CustomHeader" "X-Requested-With"})

(defmethod ig/init-key ::handler [_ opts]
  (cors/wrap-cors (handler opts)
                  :access-control-allow-headers cors-allowed-headers
                  :access-control-allow-methods [:get :options :post]
                  :access-control-allow-origin #".*"))
