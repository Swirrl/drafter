(ns drafter.timeouts
  (:require [buddy.core
             [bytes :as bytes]
             [codecs :as codecs]
             [mac :as mac]]
            [drafter.util :as util]
            [clojure.string :as string]
            [clojure.tools.logging :as log]
            [drafter.util :as util])
  (:import org.apache.commons.codec.DecoderException))

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

(defn- log-invalid-timeout-param [decoded-param error]
  (let [msg (case error
              :timeout-invalid "Invalid timeout value"
              :mac-invalid "MAC does not match"
              :format-invalid "Invalid format (expected 'timeout--MAC-hex')"
              "Unknown error")]
    (log/warn (format "Discarding maximum query timeout parameter '%s' due to invalid value: %s" decoded-param msg))))

(defn- string->mac-bytes [s mac-signing-key]
  (let [bytes (.getBytes s util/utf8)]
    (mac/hash bytes {:key mac-signing-key :alg :hmac+sha256})))

(defn gen-privileged-timeout [timeout mac-signing-key]
  {:pre [(number? timeout)]}
  (let [timeout-str (str timeout)
        mac-bytes (string->mac-bytes timeout-str mac-signing-key)
        mac-hex (codecs/bytes->hex mac-bytes)
        msg (str timeout-str "--" mac-hex)]
    (util/str->base64 msg)))

(defn try-parse-privileged-query-timeout [timeout-param mac-signing-key]
  {:pre [(or (nil? timeout-param) (string? timeout-param))]
   :post [(or (nil? %) (number? %) (keyword? %))]}
  (when timeout-param
    (let [decoded (util/base64->str timeout-param)
          [timeout-str mac-hex] (string/split decoded #"--")]
      (if (and (some? timeout-str) (some? mac-hex))
        (let [timeout-bytes (.getBytes timeout-str util/utf8)
              expected-mac-bytes (mac/hash timeout-bytes {:key mac-signing-key :alg :hmac+sha256})]
          (try
            (let [actual-mac-bytes (codecs/hex->bytes mac-hex)]
              (if (bytes/equals? expected-mac-bytes actual-mac-bytes)
                (let [timeout-or-ex (try-parse-timeout timeout-str)]
                  (if (instance? Exception timeout-or-ex)
                    :timeout-invalid
                    timeout-or-ex))
                :mac-invalid))
            (catch DecoderException ex
              :mac-invalid)))
        :format-invalid))))

(defn calculate-default-request-timeout
  "Returns a function (Request -> Timeout) which ignores the request and returns
  the default query timeout"
  [request]
  default-query-timeout)

(defn calculate-endpoint-timeout
  "Returns a function (Request -> Timeout) which calculates the endpoint timeout
  from the given timeout and default timeout."
  [endpoint-timeout inner-fn]
  (fn [request]
    (or endpoint-timeout (inner-fn request))))

(defn calculate-unprivileged-timeout
  "Returns a function (Request -> Timeout) which calculates the unprivileged timeout
   for the request. The unprivileged timeout is specified by the 'timeout' request
   parameter and can be used to reduce the endpoint timeout calculated by inner-fn.
   If the timeout parameter is invalid an Exception is returned instead of a timeout."
  [inner-fn]
  (fn [request]
    (let [endpoint-timeout (inner-fn request)
          timeout-str (get-in request [:params :timeout])
          unprivileged-timeout (some-> timeout-str (try-parse-timeout))]
      (cond
        (util/throwable? unprivileged-timeout)
        unprivileged-timeout

        (and (some? unprivileged-timeout) (some? endpoint-timeout))
        (min unprivileged-timeout endpoint-timeout)

        :else
        (or unprivileged-timeout endpoint-timeout)))))

(defn calculate-privileged-timeout
  "Returns a function (Request -> Timeout) which calculates the timeout for a request
   based on the optional privileged timeout. If specified by the max-query-timeout
   parameter the value must be signed by mac-signing-key. If the value is valid, it will
   be returned as the request timeout value. If it is invalid an Exception will be
   returned. If it is not specified on the request, the unprivileged timeout calculated
   by inner-fn will be used as the request timeout."
  [mac-signing-key inner-fn]
  (fn [request]
    (if-let [privileged-timeout-str (get-in request [:params :max-query-timeout])]
      (let [privileged-timeout (try-parse-privileged-query-timeout privileged-timeout-str mac-signing-key)]
        (if (keyword? privileged-timeout)
          (let [decoded (util/base64->str privileged-timeout-str)]
            (log-invalid-timeout-param decoded privileged-timeout)
            (IllegalArgumentException. "Invalid parameter max-query-timeout"))
          privileged-timeout))
      (inner-fn request))))

(defn calculate-request-query-timeout
  "Returns a function (Request -> Timeout) for calculating the query timeout for
   a given request. If mac-signing-key is "
  [endpoint-timeout mac-signing-key]
  (let [unprivileged-timeout-fn (->> calculate-default-request-timeout
                                     (calculate-endpoint-timeout endpoint-timeout)
                                     (calculate-unprivileged-timeout))]
    (if (nil? mac-signing-key)
      unprivileged-timeout-fn
      (calculate-privileged-timeout mac-signing-key unprivileged-timeout-fn))))
