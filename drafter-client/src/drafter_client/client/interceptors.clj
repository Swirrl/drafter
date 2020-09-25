(ns drafter-client.client.interceptors
  (:require [clojure.walk :refer [postwalk]]))

(alias 'c 'clojure.core)

(deftype DrafterClient [martian opts auth-provider auth0]
  ;; Wrap martian in a type so that:
  ;; a) we don't leak the auth0 client
  ;; b) we don't expose the martian impl to the "system"
  ;; We can still get at the pieces if necessary due to the ILookup impl.
  clojure.lang.ILookup
  (valAt [this k] (.valAt this k nil))
  (valAt [this k default]
    (case k
      :martian martian
      :auth0 auth0
      :auth-provider auth-provider
      (or (c/get opts k)
          (.valAt martian k default)))))

(defn intercept
  {:style/indent :defn}
  [{:keys [martian opts auth-provider auth0] :as client}
   & interceptors]
  (->DrafterClient (apply update martian :interceptors conj interceptors)
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
