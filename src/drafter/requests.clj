(ns drafter.requests)

(defn accept
  "Returns the Accept header for a request if one exists."
  [request]
  (get-in request [:headers "accept"]))

(defn query
  "Returns the query string for a request map"
  [request]
  (get-in request [:params :query]))

(defn get-user
  "Gets the user associated with the request if one exists"
  [request]
  (:identity request))
