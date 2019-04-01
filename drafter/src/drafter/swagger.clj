(ns drafter.swagger
  (:require [clj-yaml.core :as yaml]
            [clojure
             [string :as string]
             [walk :as walk]]
            [clojure.java.io :as io]
            [clout.core :as clout]
            [scjsv.core :as jsonsch]))

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

(defn load-spec-and-resolve-refs
  "Load our swagger schemas and inline all the JSON Pointers as a post
  processing step."
  []
  (resolve-refs (load-spec-file)))

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
