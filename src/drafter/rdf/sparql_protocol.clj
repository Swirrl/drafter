(ns drafter.rdf.sparql-protocol
  (:require [clojure.tools.logging :as log]
            [drafter.operations :refer :all]
            [drafter.requests :as request]
            [drafter.responses :refer [not-acceptable-response]]
            [drafter.backend.protocols :refer [prepare-query]]
            [drafter.rdf.sesame :refer [get-query-type create-query-executor create-signalling-query-handler]]
            [drafter.middleware :refer [allowed-methods-handler wrap-sparql-errors]]
            [drafter.channels :refer :all]
            [compojure.core :refer [make-route]]
            [drafter.rdf.content-negotiation :as conneg])
  (:import [java.io ByteArrayOutputStream PipedInputStream PipedOutputStream]
           [org.openrdf.query QueryInterruptedException]
           [org.openrdf.query.resultio QueryResultIO]
           [java.util.concurrent TimeUnit]))

(defn get-sparql-response-content-type [mime-type]
  (case mime-type
    ;; if they ask for html they're probably a browser so serve it as
    ;; text/plain
    "text/html" "text/plain; charset=utf-8"
    ;; force a charset of UTF-8 in this case... NOTE this should
    ;; really consult the Accept-Charset header
    "text/plain" "text/plain; charset=utf-8"
    mime-type))

(defn- execute-boolean-query [pquery result-format response-content-type]
  (let [os (ByteArrayOutputStream. 1024)
        writer (QueryResultIO/createWriter result-format os)]
    (with-open [_ os]
      (let [result (.evaluate pquery)]
        (.handleBoolean writer result)))
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
                  (try
                    (.evaluate pquery result-handler)
                    (catch Exception ex
                      (send ex))
                    (finally
                      (.close os))))]
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

(defn process-prepared-query [pquery accept query-timeouts]
  (let [query-type (get-query-type pquery)
        query-timeouts (or query-timeouts default-timeouts)]
    (if-let [[result-format media-type] (conneg/negotiate query-type accept)]
      (let [response-content-type (get-sparql-response-content-type media-type)]
        (.setMaxExecutionTime pquery (get-query-timeout-seconds query-timeouts))
        (if (= :ask query-type)
          (execute-boolean-query pquery result-format response-content-type)
          (execute-streaming-query pquery result-format response-content-type)))

      (not-acceptable-response))))

(defn process-sparql-query [executor request & {:keys [query-timeouts]
                                                :or {query-timeouts default-timeouts}}]
  (let [query-str (request/query request)
        pquery (prepare-query executor query-str)]
    (log/info (str "Running query\n" query-str "\nwith graph restrictions"))
    (process-prepared-query pquery (request/accept request) query-timeouts)))

(defn- sparql-query-request-handler [executor timeouts]
  (fn [request]
    (process-sparql-query executor request :query-timeouts timeouts)))

(defn sparql-protocol-handler [executor timeouts]
  (->> (sparql-query-request-handler executor timeouts)
       (wrap-sparql-errors)
       (allowed-methods-handler #{:get :post})))

(defn sparql-end-point
  "Builds a SPARQL end point from a mount-path, a SPARQL executor and
  an optional restriction function which returns a list of graph uris
  to restrict both the union and named-graph queries too."

  ([mount-path executor] (sparql-end-point mount-path executor nil))
  ([mount-path executor timeouts]
   (make-route nil mount-path (sparql-protocol-handler executor timeouts))))
