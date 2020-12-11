(ns ^:rest-api drafter.feature.draftset.create-test
  (:require [clojure.test :as t]
            [clojure.test :refer :all :as t]
            [drafter.rdf.drafter-ontology
             :refer
             [drafter:DraftGraph drafter:modifiedAt]]
            [drafter.swagger :as swagger]
            [drafter.test-common :as tc]
            [drafter.user-test :refer [test-editor test-manager test-password test-publisher]]
            [grafter-2.rdf.protocols :refer [add context ->Quad ->Triple map->Triple]]
            [grafter-2.rdf4j.io :refer [statements rdf-writer]]
            [clojure.spec.alpha :as s]))

(t/use-fixtures :each tc/with-spec-instrumentation)

(defn- contains-header? [header-name]
  (fn [response] (contains? (:headers response) header-name)))

(s/def :ring/see-other-response (s/and (tc/response-code-spec 303)
                                       (contains-header? "Location")))

(defn assert-is-see-other-response [response]
  (tc/assert-spec :ring/see-other-response response))

(defn valid-swagger-response?
  "Applies handler to request and validates the response against the
  swagger spec for the requested route.

  Returns the response if valid, otherwise raises an error."
  [handler request]
  (let [swagger-spec (swagger/load-spec-and-resolve-refs)]
    (swagger/validate-response-against-swagger-spec swagger-spec request (handler request))))

(defn create-draftset-request
  "Build a HTTP request object that representing a request to create a
  draftset."
  ([] (create-draftset-request test-editor))
  ([user] (create-draftset-request user nil))
  ([user display-name] (create-draftset-request user display-name nil))
  ([user display-name description]
   (tc/with-identity user {:uri "/v1/draftsets" :request-method :post :params {:display-name display-name :description description}})))

(tc/deftest-system-with-keys create-draftset-without-title-or-description
  [:drafter.fixture-data/loader :drafter.feature.draftset.create/handler]
  [{handler :drafter.feature.draftset.create/handler} "test-system.edn"]
  (let [request (tc/with-identity test-editor {:uri "/v1/draftsets" :request-method :post})
        response (valid-swagger-response? handler request)]
    (assert-is-see-other-response response)))

(tc/deftest-system-with-keys create-draftset-with-title-and-without-description
  [:drafter.fixture-data/loader :drafter.feature.draftset.create/handler]
  [{handler :drafter.feature.draftset.create/handler} "test-system.edn"]
  (let [response (valid-swagger-response? handler (create-draftset-request test-editor "Test Title!"))]
    (assert-is-see-other-response response)))

(tc/deftest-system-with-keys create-draftset-with-title-and-description
  [:drafter.fixture-data/loader :drafter.feature.draftset.create/handler]
  [{handler :drafter.feature.draftset.create/handler} "test-system.edn"]
  (let [response (valid-swagger-response? handler (create-draftset-request test-editor "Test title" "Test description"))]
    (assert-is-see-other-response response)))
