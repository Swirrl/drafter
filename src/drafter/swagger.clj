(ns drafter.swagger
  (:require [clj-yaml.core :as yaml]
            [scjsv.core :as jsonsch]
            [clojure.string :as string]
            [clojure.java.io :as io]
            [clojure.walk :as walk]))

(defn load-swagger-schema
  "Load our swagger schemas and inline all the JSON Pointers as a post
  processing step.  Returns a map of schema names to JSON schema
  definitions."
  []
  (let [schemas (-> "public/swagger/drafter.yml"
                    io/resource
                    slurp
                    yaml/parse-string
                    :definitions)
        update-$refs (fn [v]
                       (if (and (map? v)
                                (v :$ref))
                         (let [ref (keyword (string/replace (v :$ref)
                                                            "#/definitions/" ""))]
                           (ref schemas))
                         v))]
    (walk/postwalk update-$refs schemas)))


(defn array-of
  "Takes a JSON schema and returns a JSON schema for which there
  should be an array of schema."
  [schema]
  {:type "array" :items schema})

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

(defn make-validator
  "Return a validator function that closes over the loaded schemas.  A
  thin wrapper over validate-swagger-schema!.

  If the value does not conform to the schema then a :schema-error
  exception is raised."
  [schemas]
  (fn [schema-gen value]
    (validate-swagger-schema! (schema-gen schemas) value)))

(defn make-ring-response-validator
  "Build a validator as with make-validator, but accept a ring
  response object as an argument. If the body conforms to the
  specified schema then return the ring-response.  Otherwiwse
  a :schema-error is raised. "
  [schemas]
  (let [validate-it (make-validator schemas)]
    (fn [schema-gen ring-resp]
      (let [val (validate-it schema-gen (:body ring-resp))]
        ring-resp))))
