(ns drafter.feature.middleware
  "Shared middlewares"
  (:require [drafter.responses :refer [unprocessable-entity-response]])
  (:import java.net.URI))

(defn- try-parse-uri [s]
  (try
    (URI. s)
    (catch Exception ex
      ex)))

(defn parse-graph-param-handler
  "Handle HTTP responses associated with graph query param on draftset
  and draftset-data routes."
  [required? inner-handler]
  (fn [request]
    (let [graph (get-in request [:params :graph])]
      (cond
        (some? graph)
        (let [uri-or-ex (try-parse-uri graph)]
          (if (instance? URI uri-or-ex)
            (inner-handler (assoc-in request [:params :graph] uri-or-ex))
            (unprocessable-entity-response "Valid URI required for graph parameter")))

        required?
        (unprocessable-entity-response "Graph parameter required")

        :else
        (inner-handler request)))))
