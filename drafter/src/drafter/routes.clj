(ns drafter.routes
  "A very basic component to configure compojure routes via
  compojure/make-route.

  Essentially just takes a sequence of tuples in config and applies
  them to the compojure/make-route function.

  e.g. [:get \"/foo/bar\" #ig/ref :some/handler] would attach the ring
  handle to fire on a get request to route \"/foo/bar\"

  Also takes a :context \"/path\" option.
  "
  (:require [clojure.spec.alpha :as s]
            [compojure.core :as compojure]
            [drafter.logging :refer [with-logging-context]]
            [integrant.core :as ig]))

(defn make-route [method route handler-fn]
  (compojure/make-route method route
                        (fn [req]
                          (with-logging-context {:method method :route route}
                            (handler-fn req)))))

(defn attach-route [[method route handler]]
  (make-route method route handler))

(defn make-routes [{:keys [routes] context-path :context }]
  (let [handler (->> (map attach-route routes)
                     (apply compojure.core/routes))]
    (compojure/context context-path []
                       handler)))

(s/def ::request-method #{:get :post :put :delete :options :head nil})
(s/def ::path string?)
(s/def ::handler (s/fspec :args (s/cat :request map?)
                          :ret any?))

(s/def ::context string?)

(s/def ::route (s/tuple ::request-method ::path ::handler))

(s/def ::routes (s/+ ::route))

(s/def ::routes-config (s/keys :req-un [::routes ::context]))

(defmethod ig/init-key :drafter/routes [_ opts]
  (make-routes opts))
