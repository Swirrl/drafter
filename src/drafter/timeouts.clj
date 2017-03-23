(ns drafter.timeouts)

(defn try-parse-timeout
  "Attempts to parse a string into a timeout value. Returns an
  exception describing the error if the input cannot be parsed."
  [s]
  {:post [(or (integer? %) (instance? Exception %))]}
  (try
    (let [timeout (Integer/parseInt s)]
      (if (pos? timeout)
        timeout
        (Exception. "Timeout values must be non-negative")))
    (catch NumberFormatException ex
      (Exception. (str "Timeout value '" s "' is not an integer")))))

(def default-query-timeout
  "default timeouts for SPARQL operations (30 seconds)."
  30)

(defn calculate-query-timeout
  "Calculates the query timeout (in seconds) to use given the configured timeout
   period for the endpoint and the timeout for the executing user (if any)."
  [user-timeout endpoint-timeout]
  {:pre [(or (nil? user-timeout) (pos? user-timeout))
         (or (nil? endpoint-timeout) (pos? endpoint-timeout))]
   :post [(pos? %)]}
  (if (and (nil? user-timeout) (nil? endpoint-timeout))
    default-query-timeout
    (max (or user-timeout 0) (or endpoint-timeout 0))))
