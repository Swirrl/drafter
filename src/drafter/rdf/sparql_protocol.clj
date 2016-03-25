(ns drafter.rdf.sparql-protocol
  (:require [clojure.tools.logging :as log]
            [drafter.operations :refer :all]
            [drafter.responses :refer [not-acceptable-response]]
            [drafter.backend.protocols :refer [create-query-executor prepare-query get-query-type create-result-writer]]
            [compojure.core :refer [context defroutes routes routing let-request
                                    make-route let-routes
                                    ANY GET POST PUT DELETE HEAD]]
            [drafter.rdf.content-negotiation :as conneg]
            [ring.middleware.accept :refer [wrap-accept]])
  (:import [org.apache.jena.query QueryParseException]))

(defn mime-pref [mime q] [mime :as mime :qs q])

(defn mime-table [& preferences]
  (apply vector (apply concat preferences)))

(def tuple-query-mime-preferences
  (mime-table (mime-pref "application/sparql-results+json" 0.9)
              (mime-pref "application/sparql-results+xml" 0.9)
              (mime-pref "application/x-binary-rdf" 0.7)
              (mime-pref "text/csv" 1.0)
              (mime-pref "text/tab-separated-values" 0.8)
              (mime-pref "text/plain" 1.0)
              (mime-pref "text/html" 1.0)))

(def boolean-query-mime-preferences
  (mime-table (mime-pref "application/sparql-results+xml" 1.0)
              (mime-pref "application/sparql-results+json" 1.0)
              (mime-pref "application/x-binary-rdf" 0.7)
              (mime-pref "text/plain" 0.9)
              (mime-pref "text/html" 0.8)))

(def graph-query-mime-preferences
  (mime-table (mime-pref "application/n-triples" 1.0)
              (mime-pref "application/n-quads" 0.9)
              (mime-pref "text/n3" 0.9)
              (mime-pref "application/trig" 0.8)
              (mime-pref "application/trix" 0.8)
              (mime-pref "text/turtle" 0.9)
              (mime-pref "text/html" 0.7)
              (mime-pref "application/rdf+xml" 0.9)
              (mime-pref "text/csv" 0.8)
              (mime-pref "text/tab-separated-values" 0.7)))

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

(defn get-query-mime-preferences [query-type]
  (case query-type
    :select tuple-query-mime-preferences
    :ask boolean-query-mime-preferences
    :construct graph-query-mime-preferences
    nil))

(defn negotiate-sparql-query-mime-type [query-type request]
  (let [mime-preferences (get-query-mime-preferences query-type)
        accept-handler (wrap-accept identity {:mime mime-preferences})
        mime (get-in (accept-handler request) [:accept :mime])]
    mime))

(defn process-sparql-query [executor request & {:keys [graph-restrictions query-timeouts]
                                                :or {query-timeouts default-timeouts}}]
  (let [query-str (get-in request [:params :query])
        pquery (prepare-query executor query-str graph-restrictions)
        query-type (get-query-type executor pquery)
        accept (get-in request [:headers "accept"])]

    (log/info (str "Running query\n" query-str "\nwith graph restrictions"))

    (if-let [[result-format media-type] (conneg/negotiate query-type accept)]
      (let [writer (create-result-writer executor pquery result-format)
            exec-fn (create-query-executor executor writer pquery)
            body (stream-sparql-response exec-fn query-timeouts)
            response-content-type (get-sparql-response-content-type media-type)]
        {:status 200
         :headers {"Content-Type" response-content-type}
         ;; Designed to work with piped-input-stream this fn will be run
         ;; in another thread to stream the results to the client.
         :body body})

      (not-acceptable-response))))

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
  ([mount-path executor restrictions] (sparql-end-point mount-path executor restrictions nil))
  ([mount-path executor restrictions timeouts]
     ;; TODO make restriction-fn just the set of graphs to restrict to (or nil)
   (wrap-sparql-errors
    (routes
     (GET mount-path request
          (process-sparql-query executor request
                                :graph-restrictions restrictions
                                :query-timeouts timeouts))

     (POST mount-path request
           (process-sparql-query executor request
                                 :graph-restrictions restrictions
                                 :query-timeouts timeouts))))))
