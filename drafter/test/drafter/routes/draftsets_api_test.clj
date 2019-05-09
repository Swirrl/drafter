(ns drafter.routes.draftsets-api-test
  (:require [clojure.java.io :as io]
            [clojure.set :as set]
            [clojure.test :as t :refer :all]
            [drafter.feature.draftset.test-helper :refer :all]
            [drafter.rdf.drafter-ontology
             :refer
             [drafter:DraftGraph drafter:modifiedAt]]
            [drafter.rdf.draftset-management.job-util :as jobs]
            [drafter.rdf.sparql :as sparql]
            [drafter.swagger :as swagger]
            [drafter.test-common :as tc]
            [drafter.user :as user]
            [drafter.user-test
             :refer
             [test-editor test-manager test-password test-publisher]]
            [drafter.user.memory-repository :as memrepo]
            [drafter.util :as util]
            [grafter-2.rdf.protocols :refer [->Quad ->Triple context map->Triple]]
            [grafter-2.rdf4j.formats :as formats]
            [grafter-2.rdf4j.io :refer [statements]]
            [swirrl-server.async.jobs :refer [finished-jobs]])
  (:import java.net.URI
           java.time.OffsetDateTime
           org.eclipse.rdf4j.query.resultio.sparqljson.SPARQLResultsJSONParser))

(def ^:private ^:dynamic *route* nil)
(def ^:private ^:dynamic *user-repo* nil)

(defn- setup-route [test-function]
  (let [users (:drafter.user/memory-repository tc/*test-system*)
        swagger-spec (swagger/load-spec-and-resolve-refs)
        api-handler (:drafter.routes/draftsets-api tc/*test-system*)]

    (binding [*user-repo* users
              *route* (swagger/wrap-response-swagger-validation swagger-spec api-handler)]
      (test-function))))

(defn- route [request]
  (*route* request))

(use-fixtures :each (join-fixtures [(tc/wrap-system-setup "test-system.edn" [:drafter.user/repo :drafter.routes/draftsets-api :drafter.backend/rdf4j-repo :drafter/write-scheduler])
                                    setup-route])
  tc/with-spec-instrumentation)


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Handler tests
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn ok-response->typed-body [schema {:keys [body] :as response}]
  (tc/assert-is-ok-response response)
  (tc/assert-schema schema body)
  body)
