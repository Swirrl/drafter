(ns drafter-client.client.interceptors
  (:require [clojure.walk :refer [postwalk]]
            [drafter-client.client.protocols :as dcp]))

(defn intercept
  {:style/indent :defn}
  [{:keys [martian opts auth-provider auth0] :as client}
   & interceptors]
  (dcp/->DrafterClient (apply update martian :interceptors conj interceptors)
                   opts
                   auth-provider
                   auth0))

(defn accept [client content-type]
  (intercept client
    {:name ::content-type
     :enter (fn [ctx]
              (assoc-in ctx [:request :headers "Accept"] content-type))}))

(defn keywordize-keys
  "Recursively transforms all map keys from strings to keywords.

  Unlike martian's default keywordize-keys this one will not clobber
  clojure records."
  [m]
  (let [f (fn [[k v]] (if (string? k) [(keyword k) v] [k v]))]
    (postwalk (fn [x]
                (cond (record? x)
                      (let [ks (filter string? (keys x))]
                        (into (apply dissoc x ks) (map f x)))
                      (map? x) (into {} (map f x))
                      :else x))
              m)))

(def keywordize-params
  {:name ::keywordize-params
   :enter (fn [ctx] (update ctx :params keywordize-keys))})


(defn content-type [content-type]
  {:name ::content-type
   :enter (fn [ctx]
            (assoc-in ctx [:request :headers "Content-Type"] content-type))})

(defn set-content-type [client c-type]
  (intercept client (content-type c-type)))

(defn set-redirect-strategy [strategy]
  {:name ::set-redirect-strategy
   :enter (fn [ctx] (assoc-in ctx [:request :redirect-strategy] strategy))})

(defn set-max-redirects [n]
  {:name ::set-max-redirects
   :enter (fn [ctx] (assoc-in ctx [:request :max-redirects] n))})
