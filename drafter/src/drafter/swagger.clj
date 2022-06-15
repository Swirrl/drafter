(ns drafter.swagger
  (:require [clj-yaml.core :as yaml]
            [clojure
             [string :as string]
             [walk :as walk]]
            [clojure.java.io :as io]
            [clout.core :as clout]
            [compojure.core :refer [GET] :as compojure]
            [ring.swagger.swagger-ui :as swagger-ui]
            [scjsv.core :as jsonsch]
            [drafter.user :as user]
            [drafter.auth :as auth]
            [meta-merge.core :as mm]
            [integrant.core :as ig]))

(defn- load-spec-file []
  (-> "public/swagger/drafter.yml"
      io/resource
      slurp
      yaml/parse-string))

(defn- ref->path [ref-str]
  (->> (string/split ref-str #"/")
       (rest)
       (mapv keyword)))

(defn- resolve-refs
  "Inlines all $ref items in a swagger specification."
  [m]
  (letfn [(resolve-ref [v]
            (if (and (map? v) (contains? v :$ref))
              (let [ref-path (ref->path (:$ref v))
                    ref (get-in m ref-path)]
                ref)
              v))]
    (loop [spec m]
      (let [resolved (walk/postwalk resolve-ref spec)]
        (if (= spec resolved)
          resolved
          (recur resolved))))))

(defn get-api-route-specs [global-auth?]
  (let [read-role (when global-auth? :access)]
    [{:id 'get-endpoints
      :path "/endpoints"
      :method :get
      :role read-role}
     {:id 'get-public-endpoint
      :path "/endpoint/public"
      :method :get
      :role read-role}

     {:id 'create-draftset
      :path "/draftsets"
      :method :post
      :role :editor}
     {:id 'get-draftsets
      :path "/draftsets"
      :method :get
      :role :access}
     {:id 'get-draftset
      :path "/draftset/{id}"
      :method :get
      :role :access}
     {:id 'put-draftset
      :path "/draftset/{id}"
      :method :put
      :role :editor}
     {:id     'delete-draftset
      :path   "/draftset/{id}"
      :method :delete
      :role :editor}

     {:id 'put-draftset-graph
      :path "/draftset/{id}/graph"
      :method :put
      :role :editor}
     {:id 'delete-draftset-graph
      :path "/draftset/{id}/graph"
      :method :delete
      :role :editor}

     {:id 'delete-draftset-changes
      :path "/draftset/{id}/changes"
      :method :delete
      :role :editor}

     {:id 'put-draftset-data
      :path "/draftset/{id}/data"
      :method :put
      :role :editor}
     {:id 'delete-draftset-data
      :path "/draftset/{id}/data"
      :method :delete
      :role :editor}
     {:id 'get-draftset-data
      :path "/draftset/{id}/data"
      :method :get
      :role :editor}

     {:id 'submit-draftset-to
      :path "/draftset/{id}/submit-to"
      :method :post
      :role :editor}
     {:id 'claim-draftset
      :path "/draftset/{id}/claim"
      :method :put
      :role :editor}
     {:id 'publish-draftset
      :path "/draftset/{id}/publish"
      :method :post
      :role :publisher}

     {:id 'get-query-draftset
      :path "/draftset/{id}/query"
      :method :get
      :role :editor}
     {:id 'post-query-draftset
      :path "/draftset/{id}/query"
      :method :post
      :role :editor}
     {:id 'post-update-draftset
      :path "/draftset/{id}/update"
      :method :post
      :role :editor}
     {:id 'get-query-live
      :path "/sparql/live"
      :method :get
      :role read-role}
     {:id 'post-query-live
      :path "/sparql/live"
      :method :post
      :role read-role}

     {:id 'get-users
      :path "/users"
      :method :get
      :role :access}

     {:id 'get-job
      :path "/status/jobs/{jobid}"
      :method :get
      :role :editor}
     {:id 'get-jobs
      :path "/status/jobs"
      :method :get
      :role :editor}
     {:id 'status-job-finished
      :path "/status/finished-jobs/{jobid}"
      :method :get
      :role read-role}

     {:id 'status-writes-locked
      :path "/status/writes-locked"
      :method :get
      :role read-role}]))

(defn- route-spec-path
  "Returns the path of an operation within the swagger spec JSON"
  [{:keys [path method]}]
  [:paths (keyword path) method])

(defn- get-auth-method-security-defs [auth-method route-specs]
  (let [swagger-key (auth/get-swagger-key auth-method)
        security-def {:securityDefinitions
                      {swagger-key
                       (auth/get-swagger-security-definition auth-method)}}]
    (letfn [(authenticated? [route-spec]
              (some? (:role route-spec)))
            (add-route [spec route-spec]
              (if (authenticated? route-spec)
                (let [operation-requirements (auth/get-operation-swagger-security-requirement auth-method route-spec)]
                  (assoc-in spec (route-spec-path route-spec) {:security [{swagger-key operation-requirements}]}))
                spec))]
      (reduce add-route security-def route-specs))))

(defn- auth-method-description [auth-method]
  (let [{:keys [heading description]} (auth/get-swagger-description auth-method)]
    (str "### " heading "\n" description)))

(defn- append-authentication-method-descriptions
  "Appends authentication method documentation to the end of the swagger description"
  [spec auth-methods]
  (update-in spec [:info :description] (fn [desc]
                                         (let [sb (StringBuilder. desc)]
                                           (if (seq auth-methods)
                                             (doseq [auth-method auth-methods]
                                               (.append sb (auth-method-description auth-method))
                                               (.append sb "\n"))
                                             (.append sb "\n__No authentication methods available__"))
                                           (str sb)))))

(defn- load-spec [auth-methods global-auth?]
  (let [spec (load-spec-file)
        spec (append-authentication-method-descriptions spec auth-methods)
        route-specs (get-api-route-specs global-auth?)
        security-defs (map (fn [auth-method] (get-auth-method-security-defs auth-method route-specs)) auth-methods)]
    (reduce (fn [spec security-def] (mm/meta-merge spec security-def)) spec security-defs)))

(defn load-spec-and-resolve-refs
  "Load our swagger schemas and inline all the JSON Pointers as a post
  processing step."
  [auth-methods global-auth?]
  (resolve-refs (load-spec auth-methods global-auth?)))

(defn validate-swagger-schema!
  "Function to compare a value to its schema and raise an appropriate
  error if it fails conformance due to any schema :errors.  It will
  ignore warnings."
  [schema value]
  (let [validate (jsonsch/validator schema)
        report (validate value)]
    (if-not report
      value
      (let [errors (filter (comp #(= "error" %) :level) report)]
        (if (not-empty errors)
          (throw (ex-info (str "Drafter return value failed to conform to swagger schema.") {:error :schema-error
                                                                                             :schema-errors errors
                                                                                             :value value}))
          value)))))

(defn- swagger-path->clout-path
  "Converts a swagger path specification into the corresponding clout path used by ring for request matching.

   (swagger-path->clout-path \"/v1\" :/prefix/{type}/moar/{id})
   => /v1/prefix/:type/moar/:id"
  [basePath p]
  (let [clout-path (string/replace (name p) #"\{([\w-]+)\}" ":$1")]
    (str basePath "/" clout-path)))

(defn find-route-spec
  "Tries to match an incoming request against a route section in the given swagger specification."
  [{:keys [basePath paths] :as spec} {:keys [request-method] :as request}]
  (if-let [[path v] (first (filter (fn [[path _]]
                                     (let [cp (swagger-path->clout-path basePath path)]
                                       (clout/route-matches cp request)))
                                   paths))]
    (request-method v)))

(defn- is-client-error?
  "Returns whether the given HTTP status code represents a client error."
  [status]
  (and (>= status 400) (< status 500)))

(defn validate-response-against-swagger-spec
  "Find the swagger spec for the route specified in request and
  validate that the response conforms to what we expect."
  [spec {request-uri :uri :as request} {:keys [status body] :as response}]
  (if-let [route-spec (find-route-spec spec request)]
    (if-let [{:keys [schema] :as response-spec} (get-in route-spec [:responses (keyword (str status))])]
      ;; only validate JSON responses when schema defined in the spec
      (if (and (some? schema) (map? body))
        (do
          (validate-swagger-schema! schema body)
          response)
        response)

      ;; ignore client errors if response is not specified
      (if (is-client-error? status)
        response
        (throw (ex-info (str "Unspecified response status " status " for route: " request-uri) {:error  :schema-error
                                                                                                :status status
                                                                                                :uri    request-uri}))))
    ;; route not found in spec
    response))

(defn wrap-response-swagger-validation
  "Wraps a handler with one that validates the format of returned responses against the given swagger specification. The
  specification should have all references resolved inline into the schema. The returned handler throws an exception if:
   - the body of the response is a map which does not conform to the structure defined in the specification
   - the response status is a non-400 response which is not documented in the schema"
  [spec handler]
  (fn [request]
    (validate-response-against-swagger-spec spec request (handler request))))

(defn swagger-json-handler
  "Returns a ring handler which serves the swagger JSON at the given path. auth-methods should be a collection of all the
  configured authentication methods in the system. These are required to configure the available securityDefinitions.
  The global-auth? parameter indicates whether global authentication is enabled. This affects the roles required to
  access certain API routes."
  [path auth-methods global-auth?]
  (let [swagger-json (load-spec-and-resolve-refs auth-methods global-auth?)]
    (GET path []
      {:status 200
       :headers {"Content-Type" "application/json"}
       :body swagger-json})))

(defn swagger-ui-handler
  "Returns a handler which serves the swagger UI. json-path should be the path to the swagger JSON configuration.
   auth-methods should be the collection of configured authentication methods - these may need to configure certain aspects of the UI."
  [json-path auth-methods]
  (let [auth-method-opts (apply merge (map auth/get-swagger-ui-config auth-methods))
        opts (merge {:path "/"
                     :swagger-docs json-path}
                    auth-method-opts)]
    (swagger-ui/swagger-ui opts)))

(defn swagger-routes
  ([auth-methods global-auth?] (swagger-routes auth-methods global-auth?))
  ([auth-methods global-auth? {:keys [json-path] :or {json-path "/swagger/swagger.json"}}]
   (compojure/routes
     (swagger-json-handler json-path auth-methods global-auth?)
     (swagger-ui-handler json-path auth-methods))))

(defmethod ig/init-key ::swagger-routes [_ {:keys [auth-methods global-auth?] :as opts}]
  (swagger-routes auth-methods global-auth? opts))