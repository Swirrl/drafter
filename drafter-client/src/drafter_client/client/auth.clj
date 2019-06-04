(ns drafter-client.client.auth
  (:require [buddy.sign.jwt :as jwt]
            [clj-time.coerce :refer [to-date]]
            [clj-time.core :as time]
            [clj-time.format :refer [formatter parse]]
            [drafter-client.client.impl :as i]
            [ring.util.codec :refer [form-encode]]
            [schema.core :as schema]
            [martian.core :as martian]))

(def date-format (formatter "EEE, dd MMM yyyy HH:mm:ss zz"))

(def auth0-routes
  [{:route-name :oauth-token
    :path "/oauth/token"
    :method :post
    :path-parts ["/oauth/token"]
    :consumes ["application/x-www-form-urlencoded"]
    :produces ["application/json"]
    :summary "Get a jwt auth token from auth0"
    :body-schema {:credentials
                  {:grant_type schema/Str
                   :client_id schema/Str
                   :client_secret schema/Str
                   :audience schema/Str}}}])

(deftype Auth0Client [martian client-id client-secret]
  clojure.lang.ILookup
  (valAt [this k] (.valAt this k nil))
  (valAt [this k default]
    (case k
      :martian martian
      :client-id client-id
      :client-secret client-secret
      (.valAt martian k default))))

(defn client [endpoint client-id client-secret]
  (let [api (martian/bootstrap endpoint auth0-routes {:interceptors i/default-interceptors})]
    (->Auth0Client api client-id client-secret)))

(defn intercept
  {:style/indent :defn}
  [{:keys [martian client-id client-secret]} & interceptors]
  (->Auth0Client (apply update martian :interceptors conj interceptors)
                 client-id
                 client-secret))

(defn get-client-id-token [{:keys [auth0] :as client}]
  (let [body {:grant-type "client_credentials"
              :client-id (:client-id auth0)
              :client-secret (:client-secret auth0)
              :audience "https://drafter.swirrl.com"}]
    (-> auth0
        (intercept (i/content-type "application/x-www-form-urlencoded"))
        (intercept (i/set-redirect-strategy :none))
        (martian/response-for :oauth-token body)
        (:body))))

(defn basic-auth [user password]
  (let [bpass (.getBytes (str user \: password))]
    {:name ::auth-header
     :enter (fn [ctx]
              (assoc-in ctx [:request :headers "Authorization"]
                        (str "Basic "
                             (-> (java.util.Base64/getEncoder)
                                 (.encodeToString bpass)))))}))
