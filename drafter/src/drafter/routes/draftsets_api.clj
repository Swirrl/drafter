(ns drafter.routes.draftsets-api
  (:require [drafter.responses :refer [unprocessable-entity-response]]))

(defn parse-query-param-flag-handler [flag inner-handler]
  (fn [{:keys [params] :as request}]
    (letfn [(update-request [b] (assoc-in request [:params flag] b))]
      (if-let [value (get params flag)]
        (if (boolean (re-matches #"(?i)(true|false)" value))
          (let [ub (Boolean/parseBoolean value)]
            (inner-handler (update-request ub)))
          (unprocessable-entity-response (str "Invalid " (name flag) " parameter value - expected true or false")))
        (inner-handler (update-request false))))))

(defn parse-union-with-live-handler [inner-handler]
  (parse-query-param-flag-handler :union-with-live inner-handler))
