(ns drafter.routes.common
  (:require [drafter.test-common :as tc]
            [drafter.user-test :refer [test-editor]]
            [grafter-2.rdf.protocols :refer [add]]
            [grafter-2.rdf4j.formats :as formats]
            [grafter-2.rdf4j.io :refer [rdf-writer statements]])
  (:import [java.io ByteArrayInputStream ByteArrayOutputStream]))

(def default-sparql-query {:request-method :get :headers {"accept" "text/csv"}})

(defn- build-query
  [endpoint-path query {:keys [default-graph-uri named-graph-uri graphs]}]
  (-> default-sparql-query
      (assoc-in [:query-params "query"] query)
      (assoc :uri endpoint-path)
      (assoc-in [:query-params "default-graph-uri"] default-graph-uri)
      (assoc-in [:query-params "named-graph-uri"] named-graph-uri)))

(defn live-query [qstr & {:keys [reasoning] :as kwargs}]
  (-> "/v1/sparql/live"
      (build-query qstr kwargs)
      (cond-> reasoning (assoc-in [:query-params "reasoning"] "true"))))

(defn draftset-query
  [draftset qstr & {:keys [user union-with-live reasoning] :as kwargs}]
  (tc/with-identity (or user test-editor)
    (-> (str draftset "/query")
        (build-query qstr kwargs)
        (cond->
            union-with-live (assoc-in [:params :union-with-live] "true")
            reasoning (assoc-in [:query-params "reasoning"] "true")))))

(defn- statements->input-stream [statements format]
  (let [bos (ByteArrayOutputStream.)
        serialiser (rdf-writer bos :format format)]
    (add serialiser statements)
    (ByteArrayInputStream. (.toByteArray bos))))

(defn- append-to-draftset-request [user draftset-location data-stream content-type]
  (tc/with-identity user
    {:uri (str draftset-location "/data")
     :request-method :put
     :body data-stream
     :headers {"content-type" content-type}}))

(defn- statements->append-request [user draftset-location statements format]
  (let [input-stream (statements->input-stream statements format)]
    (append-to-draftset-request user
                                draftset-location
                                input-stream
                                (.getDefaultMIMEType (formats/->rdf-format format)))))

(defn append-quads-to-draftset-through-api [api user draftset-location quads]
  (let [request (statements->append-request user draftset-location quads :nq)
        response (api request)]
    (tc/await-success (get-in response [:body :finished-job]))))

(defn publish-draftset [api user draftset-location]
  (let [content-type (.getDefaultMIMEType (formats/->rdf-format :nq))
        request (tc/with-identity user
                  {:uri (str draftset-location "/publish")
                   :request-method :post})
        response (api request)]
    (tc/await-success (get-in response [:body :finished-job]))))
