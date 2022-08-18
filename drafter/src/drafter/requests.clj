(ns drafter.requests)

(defn accept
  "Returns the accept header or query parameter for a request if either exists.
   The query parameter has precedence over the header, since it is likely to be
   used in cases where the user has no control over the header."
  [request]
  (or (get-in request [:params :accept])
      (get-in request [:headers "accept"])))

(defn query
  "Returns the query string for a request map"
  [request]
  (get-in request [:params :query]))

(defn user-id
  "Returns the user-id for a request"
  [request]
  (get-in request [:identity :email]))
