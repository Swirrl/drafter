(ns drafter.rdf.sparql-protocol
  (:require [clojure.tools.logging :as log]
            [drafter.responses :refer [not-acceptable-response]]
            [drafter.backend.protocols :refer [prepare-query]]
            [drafter.rdf.sesame :refer [get-query-type create-query-executor create-signalling-query-handler]]
            [drafter.middleware :refer [allowed-methods-handler sparql-negotiation-handler sparql-timeout-handler sparql-prepare-query-handler require-params]]
            [drafter.channels :refer :all]
            [compojure.core :refer [make-route]])
  (:import [java.io ByteArrayOutputStream PipedInputStream PipedOutputStream]
           [org.openrdf.query QueryInterruptedException]
           [org.openrdf.query.resultio QueryResultIO]
           [java.util.concurrent TimeUnit]))

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

(defn- execute-prepared-query [pquery format response-content-type]
  (let [query-type (get-query-type pquery)]
    (try
      (if (= :ask query-type)
        (execute-boolean-query pquery format response-content-type)
        (execute-streaming-query pquery format response-content-type))
      (catch QueryInterruptedException ex
        {:status 503
         :headers {"Content-Type" "text/plain; charset=utf-8"}
         :body "Query execution timed out"}))))

(defn sparql-execution-handler [{{:keys [prepared-query format response-content-type]} :sparql :as request}]
  (log/info (str "Running query\n" prepared-query "\nwith graph restrictions"))
  (execute-prepared-query prepared-query format response-content-type))

(defn build-sparql-protocol-handler [prepare-handler exec-handler endpoint-timeout]
  (->> exec-handler
       (sparql-timeout-handler endpoint-timeout)
       (sparql-negotiation-handler)
       (prepare-handler)
       (require-params #{:query})
       (allowed-methods-handler #{:get :post})))

(defn sparql-protocol-handler [executor endpoint-timeout]
  (build-sparql-protocol-handler #(sparql-prepare-query-handler executor %) sparql-execution-handler endpoint-timeout))

(defn sparql-end-point
  "Builds a SPARQL end point from a mount-path, a SPARQL executor and
  an optional restriction function which returns a list of graph uris
  to restrict both the union and named-graph queries too."

  ([mount-path executor] (sparql-end-point mount-path executor nil))
  ([mount-path executor timeouts]
   (make-route nil mount-path (sparql-protocol-handler executor timeouts))))
