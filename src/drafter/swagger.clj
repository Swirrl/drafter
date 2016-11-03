(ns drafter.swagger
  (:require [clj-yaml.core :as yaml]
            [scjsv.core :as jsonsch]
            [clojure.string :as string]
            [clojure.java.io :as io]
            [clojure.walk :as walk]
            [clout.core :as clout]))

(defn- load-spec-file []
  (-> "public/swagger/drafter.yml"
      io/resource
      slurp
      yaml/parse-string))

(defn- ref->path [ref-str]
  (->> (string/split ref-str #"/")
       (rest)
       (mapv keyword)))

(defn- resolve-refs [m]
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

(defn- swagger-path->clout-path [basePath p]
  (let [clout-path (string/replace (name p) #"\{([\w-]+)\}" ":$1")]
    (str basePath "/" clout-path)))

(defn find-route-spec [{:keys [basePath paths]} {:keys [request-method] :as request}]
  (if-let [[path v] (first (filter (fn [[path _]]
                                     (let [cp (swagger-path->clout-path basePath path)]
                                       (clout/route-matches cp request)))
                                   paths))]
    (request-method v)))

(defn response-swagger-validation-handler [spec handler]
  (fn [request]
    (let [{:keys [status body] :as response} (handler request)]
      (if-let [route-spec (find-route-spec spec request)]
        (if-let [response-schema (get-in route-spec [:responses (keyword (str status)) :schema])]
          (if (map? body)
            (do
              (validate-swagger-schema! response-schema body)
              response)
            response)
          response)
        response))))