(ns drafter.common.api-routes
  (:require [drafter.vocabulary :refer [meta-uri]]
            [swirrl-server.responses :refer [error-response]]))

(defmacro when-params
  "Simple macro that takes a set of paramaters and tests that they're
  all truthy.  If any are falsey it returns an appropriate ring
  response with an error message.  The error message assumes that the
  symbol name is the same as the HTTP parameter name."
  [params & form]
  `(if (every? identity ~params)
     ~@form
     (error-response 400 {:msg (str "You must supply the parameters " ~(->> params
                                                                            (interpose ", ")
                                                                            (apply str)))})))

(defn meta-params
  "Given a hashmap of query parameters grab the ones prefixed meta-, strip that
  off, and turn into a URI"
  [query-params]

  (reduce (fn [acc [k v]]
            (let [k (name k)
                  param-name (subs k (+ 1 (.indexOf k "-")) (.length k))
                  new-key (meta-uri param-name)]
              (assoc acc new-key v)))
          {}
          (select-keys query-params
                       (filter (fn [p]
                                 (.startsWith (name p) "meta-"))
                               (keys query-params)))))
