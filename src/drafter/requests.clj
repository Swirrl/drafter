(ns drafter.requests)

(defn accept
  "Returns the Accept header for a request if one exists."
  [request]
  (get-in request [:headers "accept"]))
