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
                   :audience schema/Str}}}
   {:route-name :authorize
    :path "/authorize"
    :method :get
    :path-parts ["/authorize"]
    :summary "Get an auth-code from auth0"
    :query-schema {:response_type schema/Str
                   :client_id schema/Str
                   (schema/optional-key :client_secret) schema/Str
                   :redirect_uri schema/Str
                   :scope schema/Str
                   (schema/optional-key :audience) schema/Str
                   :state schema/Str}}])

(deftype Auth0Client [martian token client-id client-secret redirect-uri]
  clojure.lang.ILookup
  (valAt [this k] (.valAt this k nil))
  (valAt [this k default]
    (case k
      :martian martian
      :token token
      :client-id client-id
      :client-secret client-secret
      :redirect-uri redirect-uri
      (.valAt martian k default))))

(defn client
  ([endpoint client-id client-secret]
   (client endpoint client-id client-secret nil))
  ([endpoint client-id client-secret redirect-uri]
   (let [api (martian/bootstrap endpoint
                                auth0-routes
                                {:interceptors i/default-interceptors})]
     (->Auth0Client api (atom nil) client-id client-secret redirect-uri))))

(defn intercept
  {:style/indent :defn}
  [{:keys [martian token client-id client-secret redirect-uri]} & interceptors]
  (->Auth0Client (apply update martian :interceptors conj interceptors)
                 token
                 client-id
                 client-secret
                 redirect-uri))

(defn auth0-body
  ([auth0]
   {:grant-type "client_credentials"
    :client-id (:client-id auth0)
    :client-secret (:client-secret auth0)
    :audience "https://drafter.swirrl.com"})
  ([auth0 auth-code redirect-uri]
   {:grant-type "authorization_code"
    :code auth-code
    :client-id (:client-id auth0)
    :client-secret (:client-secret auth0)
    :redirect-uri redirect-uri}))

(defn- get-auth0-token* [auth0 body]
  (-> auth0
      (intercept (i/content-type "application/x-www-form-urlencoded"))
      (martian/response-for :oauth-token body)
      (:body)))

(defn get-auth0-token
  ([{:keys [auth0] :as client}]
   (get-auth0-token* auth0 (auth0-body auth0)))
  ([{:keys [auth0] :as client} auth-code]
   (get-auth0-token* auth0 (auth0-body auth0 auth-code (:redirect-uri auth0)))))

;; (prn
;;  (-> {:auth0 (client "https://dev-kkt-m758.eu.auth0.com"
;;                      "OBLNm87hPk3MwXJRFgZMqIKGWEOaedL5"
;;                      "w6LsvEK8vekUZu8GmHqzPTvb_9-8bXEvMpfgrTUXdGvqTgI6LlhhRXgBeAejKQIZ")}
;;      (get-auth0-token)))



(defn auth0 [client & [user]]
  {:name ::auth0
   :enter (fn [ctx]
            (let [token (-> client :auth0 :token)
                  {:keys [access_token token_type]}
                  (or @token
                      (reset! token (get-auth0-token client)))]
              (prn token)
              (assoc-in ctx
                        [:request :headers "Authorization"]
                        (format "%s: %s" token_type access_token))))})

(defn basic-auth [user password]
  (let [bpass (.getBytes (str user \: password))]
    {:name ::auth-header
     :enter (fn [ctx]
              (assoc-in ctx [:request :headers "Authorization"]
                        (str "Basic "
                             (-> (java.util.Base64/getEncoder)
                                 (.encodeToString bpass)))))}))

(defn authorize [{:keys [auth0] :as client}]
  (let [body {:response-type "code"
              :client-id (:client-id auth0)
              ;; :client-secret (:client-secret auth0)
              :redirect-uri (:redirect-uri auth0)
              :scope "openid"
              ;; :audience "https://drafter.swirrl.com"
              :state "12345"}]
    (-> auth0
        (intercept (i/set-max-redirects 1))
        (intercept (i/set-redirect-strategy :graceful))
        (martian/response-for :authorize body))))

(comment

  (clojure.pprint/pprint
  (-> {:auth0 (client "https://dev-kkt-m758.eu.auth0.com"
                      "LtjAgnccNzv0tM3he7RtDHui1T2w615n"
                      "ri6t1nVxNAg5ueDz3V4dBX_ucNNBXXKBgCGdo4aYJ_PE9FDFd26BrRu50Xam91P2"
                      "https://swirrl.com/login"
                      )}
      authorize))

  )

(def jws-auth (fn [& args]))
