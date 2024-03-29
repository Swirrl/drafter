(ns drafter.feature.draftset.query
  (:require [clojure.spec.alpha :as s]
            [drafter.backend :as backend]
            [drafter.routes.draftsets-api :refer [parse-union-with-live-handler]]
            [drafter.rdf.sparql-protocol :as sp :refer [sparql-protocol-handler]]
            [ring.middleware.cors :as cors]
            [drafter.errors :refer [wrap-encode-errors]]
            [integrant.core :as ig]))

(defn handler
  [{backend :drafter/backend :keys [wrap-as-draftset-viewer timeout-fn]}]
  (wrap-as-draftset-viewer :drafter:draft:view
   (parse-union-with-live-handler
    (fn [{{:keys [draftset-id union-with-live]} :params :as request}]
      (let [executor (backend/endpoint-repo backend draftset-id {:union-with-live? union-with-live})
            handler (sparql-protocol-handler {:repo executor :timeout-fn timeout-fn})]
        (handler request))))))

(defmethod ig/pre-init-spec ::handler [_]
  (s/keys :req [:drafter/backend]
          :req-un [::wrap-as-draftset-viewer ::sp/timeout-fn]))

(def cors-allowed-headers
  #{"Accept"
    "Accept-Encoding"
    "Authorization"
    "Cache-Control"
    "Content-Type"
    "DNT"
    "If-Modified-Since"
    "Keep-Alive"
    "User-Agent"
    "X-CustomHeader"
    "X-Requested-With"})

(defmethod ig/init-key ::handler [_ opts]
  (-> (handler opts)
      (wrap-encode-errors)
      (cors/wrap-cors :access-control-allow-headers cors-allowed-headers
                      :access-control-allow-methods [:get :options :post]
                      :access-control-allow-origin #".*")))
