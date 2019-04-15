(ns drafter.feature.draftset.create-test
  (:require [drafter.feature.draftset.create :as sut]
            [clojure.test :as t])
  (:require [clojure.java.io :as io]
            [clojure.set :as set]
            [clojure.test :refer :all :as t]
            [drafter.middleware :as middleware]
            [drafter.rdf.drafter-ontology
             :refer
             [drafter:DraftGraph drafter:modifiedAt]]
            [drafter.rdf.draftset-management.job-util :as jobs]
            [drafter.rdf.sparql :as sparql]
            [drafter.swagger :as swagger]
            [drafter.test-common :as tc]
            [drafter.timeouts :as timeouts]
            [drafter.user :as user]
            [drafter.user-test :refer [test-editor test-manager test-password test-publisher]]
            [drafter.user.memory-repository :as memrepo]
            [drafter.util :as util]
            [grafter-2.rdf.protocols :refer [add context ->Quad ->Triple map->Triple]]
            [grafter-2.rdf4j.formats :as formats]
            [grafter-2.rdf4j.io :refer [statements rdf-writer]]
            [schema.core :as s]
            [swirrl-server.async.jobs :refer [finished-jobs]]))

(t/use-fixtures :each tc/with-spec-instrumentation)

(def see-other-response-schema
  (merge tc/ring-response-schema
         {:status (s/eq 303)
          :headers {(s/required-key "Location") s/Str}}))



(defn assert-is-see-other-response [response]
  (tc/assert-schema see-other-response-schema response))

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
