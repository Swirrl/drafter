(ns drafter.rdf.sparql-protocol
  (:require [clojure.tools.logging :as log]
            [drafter.operations :refer :all]
            [drafter.requests :as request]
            [drafter.responses :refer [not-acceptable-response]]
            [drafter.backend.protocols :refer [create-query-executor prepare-query get-query-type]]
            [compojure.core :refer [routes GET POST]]
            [drafter.rdf.content-negotiation :as conneg])
  (:import [org.apache.jena.query QueryParseException]))

;result-streamer :: (OutputStream -> NotifierFn -> ()) -> NotifierFn -> (OutputStream -> ())
(defn result-streamer [exec-fn result-notify-fn]
  "Returns a function that handles the errors and closes the SPARQL
  results stream when it's done.

  If an error is thrown the stream will be closed and an exception
  logged."
  (fn [ostream]
    (try
      (exec-fn ostream result-notify-fn)

      (catch Exception ex
        ;; Note that if we error here it's now too late to return a
        ;; HTTP RESPONSE code error
        (log/error ex "Error streaming results"))

      (finally
        (.close ostream)))))

(defn get-sparql-response-content-type [mime-type]
  (case mime-type
    ;; if they ask for html they're probably a browser so serve it as
    ;; text/plain
    "text/html" "text/plain; charset=utf-8"
    ;; force a charset of UTF-8 in this case... NOTE this should
    ;; really consult the Accept-Charset header
    "text/plain" "text/plain; charset=utf-8"
    mime-type))

(defn stream-sparql-response [exec-fn query-timeouts]
  (let [{:keys [publish] :as query-operation} (create-operation)
        streamer (result-streamer exec-fn publish)
        [write-fn input-stream] (connect-piped-output-stream streamer)]

      (execute-operation query-operation write-fn query-timeouts)
      input-stream))

(defn process-prepared-query [executor pquery accept query-timeouts]
  (let [query-type (get-query-type executor pquery)
        query-timeouts (or query-timeouts default-timeouts)]
    (if-let [[result-format media-type] (conneg/negotiate query-type accept)]
      (let [exec-fn (create-query-executor executor result-format pquery)
            body (stream-sparql-response exec-fn query-timeouts)
            response-content-type (get-sparql-response-content-type media-type)]
        {:status 200
         :headers {"Content-Type" response-content-type}
         ;; Designed to work with piped-input-stream this fn will be run
         ;; in another thread to stream the results to the client.
         :body body})

      (not-acceptable-response))))

(defn process-sparql-query [executor request & {:keys [query-timeouts]
                                                :or {query-timeouts default-timeouts}}]
  (let [query-str (request/query request)
        pquery (prepare-query executor query-str)]
    (log/info (str "Running query\n" query-str "\nwith graph restrictions"))
    (process-prepared-query executor pquery (request/accept request) query-timeouts)))

(defn wrap-sparql-errors [handler]
  (fn [request]
    (try
      (handler request)
      (catch QueryParseException ex
        (let [error-message (.getMessage ex)]
          (log/info "Malformed query: " error-message)
          {:status 400 :headers {"Content-Type" "text/plain; charset=utf-8"} :body error-message})))))

(defn sparql-end-point
  "Builds a SPARQL end point from a mount-path, a SPARQL executor and
  an optional restriction function which returns a list of graph uris
  to restrict both the union and named-graph queries too."

  ([mount-path executor] (sparql-end-point mount-path executor nil))
  ([mount-path executor timeouts]
     ;; TODO make restriction-fn just the set of graphs to restrict to (or nil)
   (wrap-sparql-errors
    (routes
     (GET mount-path request
          (process-sparql-query executor request                        
                                :query-timeouts timeouts))

     (POST mount-path request
           (process-sparql-query executor request                        
                                 :query-timeouts timeouts))))))
