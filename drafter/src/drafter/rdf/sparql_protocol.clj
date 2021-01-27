(ns drafter.rdf.sparql-protocol
  (:require [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log]
            [clojure.zip :as z]
            [cognician.dogstatsd :as datadog]
            [compojure.core :refer [make-route]]
            [drafter.backend.common :as bcom]
            [drafter.backend.draftset.arq :as arq]
            [drafter.channels :refer :all]
            [drafter.rdf.content-negotiation :as conneg]
            [drafter.rdf.sesame
             :as
             ses
             :refer
             [create-signalling-query-handler get-query-type]]
            [drafter.requests :as drafter-request]
            [drafter.responses :as response]
            [drafter.timeouts :as timeouts]
            [grafter-2.rdf4j.repository :as repo]
            [integrant.core :as ig]
            [ring.util.request :as request])
  (:import clojure.lang.ExceptionInfo
           [java.io ByteArrayOutputStream PipedInputStream PipedOutputStream]
           java.net.SocketTimeoutException
           java.util.concurrent.TimeUnit
           [org.apache.jena.query QueryFactory QueryParseException Syntax]
           org.apache.jena.sparql.sse.Item
           org.eclipse.rdf4j.query.QueryInterruptedException
           org.eclipse.rdf4j.query.resultio.QueryResultIO))

(defn- parse-reasoning [{params :query-params :as request}]
  (let [param (or (get params "reasoning") (get params "infer"))]
    (if (string? param)
      (Boolean/parseBoolean param)
      false)))

(defn sparql-prepare-query-handler
  "Returns a ring handler which fetches the query string from an
  incoming request, validates it and prepares it using the given
  executor. The unvalidated query string should exist at the
  path [:sparql :query-string] in the incoming request. The prepared
  query is associated into the request at
  the [:sparql :prepared-query] key for access in downstream
  handlers."
  [executor inner-handler]
  (fn [{:keys [sparql] :as request}]
    (with-open [conn (repo/->connection executor)]
      (try
        (let [validated-query-str (bcom/validate-query (get sparql :query-string))
              pquery (repo/prepare-query conn
                                         validated-query-str
                                         (bcom/user-dataset sparql))]
          (when-let [reasoning (parse-reasoning request)]
            (doto pquery (.setIncludeInferred reasoning)))
          (inner-handler (assoc-in request [:sparql :prepared-query] pquery)))
        (catch QueryParseException ex
          (let [error-message (.getMessage ex)]
            (log/info "Malformed query: " error-message)
            {:status 400 :headers {"Content-Type" "text/plain; charset=utf-8"} :body error-message}))))))

(defn sparql-constant-prepared-query-handler
  "Returns a handler which associates the given prepared SPARQL query
  into the request at the key expected by later stages in the SPARQL
  processing pipeline."
  [pquery inner-handler]
  (fn [request]
    (when-let [reasoning (parse-reasoning request)]
      (doto pquery (.setIncludeInferred reasoning)))
    (inner-handler (assoc-in request [:sparql :prepared-query] pquery))))

(defn get-sparql-response-content-type [mime-type]
  (case mime-type
    ;; if they ask for html they're probably a browser so serve it as
    ;; text/plain
    "text/html" "text/plain; charset=utf-8"
    ;; force a charset of UTF-8 in this case... NOTE this should
    ;; really consult the Accept-Charset header
    "text/plain" "text/plain; charset=utf-8"
    mime-type))

(defn sparql-negotiation-handler
  "Performs content negotiation on an incoming SPARQL request and
  associates the sesame format and response content type into the
  outgoing request into the :format and :response-content-type keys
  respecitvely within the :sparql map. This handler expects to find
  the prepared sesame query at the path [:sparql :prepared-query]
  within the incoming request map - prepare-sparql-query-handler is a
  handler which populates this value."
  [inner-handler]
  (fn [request]
    (let [pquery (get-in request [:sparql :prepared-query])
          accept (drafter-request/accept request)]
      (let [query-type (ses/get-query-type pquery)]
        (if-let [[result-format media-type] (conneg/negotiate query-type accept)]
          (let [to-assoc {:format                result-format
                          :response-content-type (get-sparql-response-content-type media-type)}
                updated-request (update request :sparql #(merge % to-assoc))]
            (inner-handler updated-request))
          (response/not-acceptable-response))))))

(defn allowed-methods-handler
  "Wraps a handler with one which checks whether the method of the
  incoming request is allowed with a given predicate. If the request
  method does not pass the predicate a 405 Not Allowed response is
  returned."
  [is-allowed-fn inner-handler]
  (fn [{:keys [request-method] :as request}]
    (if (is-allowed-fn request-method)
      (inner-handler request)
      (response/method-not-allowed-response request-method))))

(defn sparql-query-parser-handler [inner-handler]
  (fn [{:keys [request-method body query-params form-params] :as request}]
    (letfn [(handle [query-string default-graph-uri named-graph-uri location]
              (cond
                (string? query-string)
                (-> request
                    (assoc-in [:sparql :query-string] query-string)
                    (assoc-in [:sparql :default-graph-uri] default-graph-uri)
                    (assoc-in [:sparql :named-graph-uri] named-graph-uri)
                    (inner-handler))

                (instance? java.io.InputStream query-string)
                (handle (slurp query-string) default-graph-uri named-graph-uri location)

                (coll? query-string)
                (response/unprocessable-entity-response "Exactly one query parameter required")

                :else
                (response/unprocessable-entity-response (str "Expected SPARQL query in " location))))]
      (case request-method
        :get (handle (get query-params "query")
                     (get query-params "default-graph-uri")
                     (get query-params "named-graph-uri")
                     "'query' query string parameter")
        :post (case (request/content-type request)
                "application/x-www-form-urlencoded"
                (handle (get form-params "query")
                        (get form-params "default-graph-uri")
                        (get form-params "named-graph-uri")
                        "'query' form parameter")
                "application/sparql-query"
                (handle body
                        (get query-params "default-graph-uri")
                        (get query-params "named-graph-uri")
                        "body")
                (let [params (:params request)]
                  (log/warn "Handling SPARQL POST query with missing content type")
                  (handle (get params :query)
                          (get params :default-graph-uri)
                          (get params :named-graph-uri)
                          "'query' form or query parameter")))
        (response/method-not-allowed-response request-method)))))

(defn disallow-sparql-service-db-uri*
  [handler {{q :query-string} :sparql :as request}]
  (letfn [(service-node? [^Item n]
            (and (.isList n)
                 (if-let [[op arg] (seq (.getList n))]
                   (and (.isSymbol op) (= (.getSymbol op) "service"))
                   false)))
          (valid? [ssez]
            (let [ssez'
                  (loop [ssez ssez]
                    (cond (service-node? (z/node ssez))
                          (z/replace (z/up ssez)
                                     {:type ::service-node-present-in-query})
                          (z/end? ssez)
                          (z/root ssez)
                          :else
                          (recur (z/next ssez))))]
              (not (and (sequential? ssez')
                        (= (:type (first ssez'))
                           ::service-node-present-in-query)))))]
    (let [query (try
                  (QueryFactory/create q Syntax/syntaxSPARQL_11)
                  (catch Exception _))
          zipper (some-> query arq/->sse-item arq/sse-zipper)]
      (if (or (nil? zipper) (valid? zipper))
        ;; a `(nil? zipper)` means that the query was unable to be parsed, but
        ;; this middleware does not deal with that
        (handler request)
        {:status 400
         :headers {"Content-Type" "text/plain; charset=utf-8"}
         :body "Cannot use SERVICE keyword in query"}))))

(defn disallow-sparql-service-db-uri [handler]
  (fn [request]
    (disallow-sparql-service-db-uri* handler request)))

(defn- execute-boolean-query [pquery result-format response-content-type]
  (let [os (ByteArrayOutputStream. 1024)
        writer (QueryResultIO/createWriter result-format os)]
    (with-open [_ os]
      (datadog/measure! "drafter.sparql.query.time" {}
         (let [result (.evaluate pquery)]
           (.handleBoolean writer result))))
    {:status 200
     :headers {"Content-Type" response-content-type}
     :body (String. (.toByteArray os))}))

(defn- execute-streaming-query [pquery result-format response-content-type]
  (let [is (PipedInputStream.)
        os (PipedOutputStream. is)
        [send recv] (create-send-once-channel)
        result-handler (create-signalling-query-handler pquery os result-format send)
        timeout (inc (.getMaxExecutionTime pquery))
        ;;start query execution in its own thread
        ;;once results have been recieved
        query-f (future
                  (let [start-time (System/currentTimeMillis)]
                    (try
                      (.evaluate pquery result-handler)
                      (catch Exception ex
                        (send ex))
                      (finally
                        (.close os)
                        (let [end-time (System/currentTimeMillis)]
                          (datadog/histogram! "drafter.sparql.query.time" (- end-time start-time)))))))]
    ;;wait for signal from query execution thread that the response has been received and begun streaming
    ;;results
    ;;NOTE: This could block for as long as the query execution timeout period
    (let [result (recv timeout TimeUnit/SECONDS)]
      (cond
        ;;began streaming results
        (channel-ok? result)
        {:status 200 :headers {"Content-Type" response-content-type} :body is}

        ;;error while executing query
        (channel-error? result)
        (throw result)

        :else
        (do
          (assert (channel-timeout? result))
          ;;.poll timed out without recieving a signal from the query thread
          ;;cancel the operation and return a timeout response
          (future-cancel query-f)
          (throw (QueryInterruptedException.)))))))

(def timeout-response
  {:status 503
   :headers {"Content-Type" "text/plain; charset=utf-8"}
   :body "Query execution timed out"})

(defn- execute-prepared-query [pquery format response-content-type]
  (let [query-type (get-query-type pquery)]
    (try
      (if (= :ask query-type)
        (execute-boolean-query pquery format response-content-type)
        (execute-streaming-query pquery format response-content-type))
      (catch QueryInterruptedException ex
        timeout-response)
      (catch SocketTimeoutException ex
        timeout-response))))

(defn sparql-execution-handler [{{:keys [prepared-query format response-content-type]} :sparql :as request}]
  (log/info (str "Running query\n" prepared-query "\nwith graph restrictions"))
  (execute-prepared-query prepared-query format response-content-type))

(defn sparql-timeout-handler
  "Returns a handler which configures the timeout for the prepared SPARQL query associated with the request.
   The timeout is calculated based on the optional timeout and
  max-query-timeout parameters on the request along with the timeout
  specified for the endpoint."
  [calculate-timeout-fn inner-handler]
  (fn [{{pquery :prepared-query} :sparql :as request}]
    (let [timeout-or-ex (calculate-timeout-fn request)]
      (if (instance? Exception timeout-or-ex)
        {:status 400 :headers {"Content-Type" "text/plain; charset=utf-8"} :body (.getMessage timeout-or-ex)}
        (let [query-timeout timeout-or-ex]
          (.setMaxExecutionTime pquery query-timeout)
          (inner-handler (assoc-in request [:sparql :timeout] query-timeout)))))))

(defn build-sparql-protocol-handler [prepare-handler exec-handler query-timeout-fn]
  (->> exec-handler
       (sparql-timeout-handler query-timeout-fn)
       (sparql-negotiation-handler)
       (prepare-handler)
       (disallow-sparql-service-db-uri)
       (sparql-query-parser-handler)))

(def default-query-timeout-fn (fn [request] timeouts/default-query-timeout))

(s/def ::timeout-fn fn?)

(defn sparql-protocol-handler
  "Builds a SPARQL endpoint from a SPARQL executor/repo and a
  timeout-fn.  The handler is not mounted to a specific route/path."
  [{:keys [repo timeout-fn]}]
  (build-sparql-protocol-handler #(sparql-prepare-query-handler repo %) sparql-execution-handler timeout-fn))

(defmethod ig/init-key ::handler [_ opts]
  (sparql-protocol-handler opts))

(defn sparql-end-point
  "Builds a SPARQL end point from a mount-path, a SPARQL executor and
  an optional restriction function which returns a list of graph uris
  to restrict both the union and named-graph queries too."

  ([mount-path executor] (sparql-end-point mount-path executor default-query-timeout-fn))
  ([mount-path executor query-timeout-fn]
   (make-route nil mount-path (sparql-protocol-handler {:repo executor :timeout-fn query-timeout-fn}))))
