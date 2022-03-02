(ns ^:rest-api drafter.feature.draftset-data.delete-test
  (:require [clojure.java.io :as io]
            [drafter.user-test :refer [test-editor test-manager test-password test-publisher]]
            [clojure.test :as t]
            [drafter.test-common :as tc]
            [drafter.user-test :refer [test-editor]]
            [drafter.feature.draftset.test-helper :as help]
            [grafter-2.rdf4j.io :as gio]
            [drafter.async.jobs :as async]
            [grafter-2.rdf4j.formats :as formats]
            [clojure.set :as set]
            [grafter-2.rdf.protocols :as gproto]
            [drafter.rdf.drafter-ontology :refer [drafter:endpoints drafter:public drafter:Endpoint]]
            [grafter.vocabularies.rdf :refer [rdf:a]]
            [grafter.vocabularies.dcterms :refer [dcterms:issued dcterms:modified]]
            [drafter.feature.endpoint.public :as pub]
            [drafter.rdf.sesame :as ses])
  (:import java.net.URI
           org.eclipse.rdf4j.rio.RDFFormat))

(t/use-fixtures :each tc/with-spec-instrumentation)

(def system-config "drafter/feature/empty-db-system.edn")

(t/deftest delete-public-endpoint-quads-test
  (tc/with-system
    [:drafter.fixture-data/loader [:drafter/routes :draftset/api] :drafter/write-scheduler]
    [system system-config]
    (do
      (pub/ensure-public-endpoint (:drafter/backend system))
      (tc/check-endpoint-graph-consistent
        system
        (let [handler (get system [:drafter/routes :draftset/api])
              draftset-location (help/create-draftset-through-api handler test-publisher)
              {:keys [created-at updated-at]} (help/get-public-endpoint-through-api handler)
              to-delete [(gproto/->Quad drafter:public rdf:a drafter:Endpoint drafter:endpoints)
                         (gproto/->Quad drafter:public dcterms:modified updated-at drafter:endpoints)
                         (gproto/->Quad drafter:public dcterms:issued created-at drafter:endpoints)]]
          ;;NOTE: endpoints graph is not managed so attempting to delete statements from it
          ;;is a no-op
          (help/delete-quads-through-api handler test-publisher draftset-location to-delete))))))

(t/deftest delete-public-endpoint-triples-test
  (tc/with-system
    [:drafter.fixture-data/loader [:drafter/routes :draftset/api] :drafter/write-scheduler]
    [system system-config]
    (do
      (pub/ensure-public-endpoint (:drafter/backend system))
      (tc/check-endpoint-graph-consistent system
        (let [handler (get system [:drafter/routes :draftset/api])
              draftset-location (help/create-draftset-through-api handler test-publisher)
              {:keys [created-at updated-at]} (help/get-public-endpoint-through-api handler)
              to-delete [(gproto/->Triple drafter:public rdf:a drafter:Endpoint)
                         (gproto/->Triple drafter:public dcterms:issued created-at)
                         (gproto/->Triple drafter:public dcterms:modified updated-at)]]
          ;;NOTE: endpoints graph is not managed so attempting to delete statements from it
          ;;is a no-op
          (help/delete-triples-through-api handler test-publisher draftset-location to-delete drafter:endpoints)
          (help/publish-draftset-through-api handler draftset-location test-publisher))))))

(tc/deftest-system-with-keys delete-draftset-data-for-non-existent-draftset
  [:drafter.fixture-data/loader [:drafter/routes :draftset/api]]
  [system system-config]
  (let [handler (get system [:drafter/routes :draftset/api])]
    (with-open [fs (io/input-stream "test/resources/test-draftset.trig")]
      (let [delete-request (tc/with-identity test-manager {:uri "/v1/draftset/missing/data" :request-method :delete :body fs})
            delete-response (handler delete-request)]
        (tc/assert-is-not-found-response delete-response)))))

(tc/deftest-system-with-keys delete-draftset-data-request-with-unknown-content-type
  [:drafter.fixture-data/loader [:drafter/routes :draftset/api]]
  [system system-config]
  (let [handler (get system [:drafter/routes :draftset/api])]
    (with-open [input-stream (io/input-stream "test/resources/test-draftset.trig")]
      (let [draftset-location (help/create-draftset-through-api handler test-editor)
            delete-request (help/create-delete-quads-request test-editor
                                                             draftset-location
                                                             input-stream
                                                             {:content-type "application/unknown-quads-format"})
            delete-response (handler delete-request)]
        (tc/assert-is-unsupported-media-type-response delete-response)))))

(t/deftest delete-gzipped-draftset-data-test
  (tc/with-system
    [:drafter.fixture-data/loader :drafter/write-scheduler [:drafter/routes :draftset/api]]
    [system system-config]
    (let [handler (get system [:drafter/routes :draftset/api])
          data-file-path "test/resources/test-draftset.trig"
          quads (gio/statements data-file-path)
          draftset-location (help/create-draftset-through-api handler test-editor)]
      (help/append-quads-to-draftset-through-api handler test-editor draftset-location quads)

      (let [quads-to-delete (map (fn [kvp] (first (val kvp))) (group-by :c quads))
            fmt (formats/->rdf-format :nq)
            ss (help/statements->gzipped-input-stream quads-to-delete fmt)
            delete-request (help/create-delete-quads-request test-editor
                                                             draftset-location
                                                             ss
                                                             {:content-type (.getDefaultMIMEType fmt)})
            delete-request (assoc-in delete-request [:headers "content-encoding"] "gzip")
            delete-response (handler delete-request)]
        (tc/await-success (get-in delete-response [:body :finished-job]))

        (let [ds-quads (help/get-user-draftset-quads-through-api handler draftset-location test-editor)
              expected (help/eval-statements (set/difference (set quads) (set quads-to-delete)))]
          (t/is (= (set expected) (set ds-quads))))))))

(tc/deftest-system-with-keys delete-draftset-data-with-metadata-test
  [:drafter.fixture-data/loader [:drafter/routes :draftset/api] :drafter/write-scheduler]
  [system system-config]
  (let [handler (get system [:drafter/routes :draftset/api])
        quads-path "test/resources/test-draftset.trig"
        triples-path "./test/test-triple.nt"
        draftset-location (help/create-draftset-through-api handler test-editor)]

    (t/testing "Deleting quads with metadata"
      (let [quads (gio/statements quads-path)
            delete-request (help/create-delete-statements-request test-editor
                                                                  draftset-location
                                                                  quads
                                                                  {:format :trig
                                                                   :metadata {:title "Custom job title"}})
            delete-response (handler delete-request)]
        (tc/await-success (get-in delete-response [:body :finished-job]))

        (let [job (-> delete-response :body :finished-job tc/job-path->job-id async/complete-job)]
          (t/is (= #{:title :draftset :operation} (-> job :metadata keys set)))
          (t/is (= "Custom job title" (-> job :metadata :title))))))

    (t/testing "Deleting triples with metadata"
      (let [triples (gio/statements triples-path)
            delete-request (help/create-delete-triples-request test-editor
                                                               draftset-location
                                                               triples
                                                               {:graph (URI. "http://graph-uri")
                                                                :metadata {:title "Custom job title"}})
            delete-response (handler delete-request)]
        (tc/await-success (get-in delete-response [:body :finished-job]))

        (let [job (-> delete-response :body :finished-job tc/job-path->job-id async/complete-job)]
          (t/is (= #{:title :draftset :operation} (-> job :metadata keys set)))
          (t/is (= "Custom job title" (-> job :metadata :title))))))))
