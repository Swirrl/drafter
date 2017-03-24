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

(defn- lift-nil [f]
  (fn [& args]
    (if-let [somes (seq (remove nil? args))]
      (apply f somes))))

(defn- nil-or-pos? [x]
  (or (nil? x) (pos? x)))

(defn calculate-query-timeout
  "Calculates the query timeout (in seconds) to use given the timeout specified on the current query,
   the configured timeout period for the endpoint and the timeout for the executing user (if any).
   - if all parameters are nil the default timeout is used
   - the user timeout increases the timeout configured on the endpoint
   - the query timeout can reduce the maximum timeout if specified"
  [query-timeout user-timeout endpoint-timeout]
  {:pre [(nil-or-pos? query-timeout)
         (nil-or-pos? user-timeout)
         (nil-or-pos? endpoint-timeout)]
   :post [(pos? %)]}
  (-> ((lift-nil max) user-timeout endpoint-timeout)
       ((lift-nil min) query-timeout)
       (or default-query-timeout)))
