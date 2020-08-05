(ns drafter.feature.endpoint.list
  (:require [integrant.core :as ig]
            [drafter.endpoint :as ep]
            [drafter.feature.endpoint.public :as pub]
            [drafter.feature.draftset.list :as dsl]
            [drafter.middleware :refer [include-endpoints-param maybe-authenticated]]
            [drafter.routes.draftsets-api :refer [parse-union-with-live-handler]]
            [clojure.spec.alpha :as s]
            [ring.util.response :as ring]))

(defn user [request] (:identity request))

(defn- public-endpoints
  "Returns all the public endpoints which satisfy the include constraint. The public
   endpoint is not owned or claimable by any user."
  [repo include]
  (if (= :all include)
    (if-let [pe (pub/get-public-endpoint repo)]
      [pe])))

(defn get-endpoints
  "Returns a sequence of all the endpoints visible to a user which satisfy the include
   constraint."
  [repo user include union-with-live?]
  (let [public (public-endpoints repo include)
        draftsets (if user
                    (dsl/get-draftsets repo user include union-with-live?))]
    (concat public draftsets)))

(defn list-handler
  ":get /endpoints"
  [backend wrap-authenticated]
  (maybe-authenticated
    wrap-authenticated
    (include-endpoints-param
      (parse-union-with-live-handler
        (fn [{user :identity {:keys [include union-with-live]} :params :as request}]
          (ring/response (get-endpoints backend user include union-with-live)))))))

(defmethod ig/init-key ::handler [_ {:keys [drafter/backend wrap-auth] :as opts}]
  (list-handler backend wrap-auth))
