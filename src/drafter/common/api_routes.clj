(ns drafter.common.api-routes)

(defmacro when-params
  "Simple macro that takes a set of paramaters and tests that they're
  all truthy.  If any are falsey it returns an appropriate ring
  response with an error message.  The error message assumes that the
  symbol name is the same as the HTTP parameter name."
  [params & form]
  `(if (every? identity ~params)
     ~@form
     (api-routes/error-response 400 {:msg (str "You must supply the parameters " ~(->> params
                                                                            (interpose ", ")
                                                                            (apply str)))})))
(def default-response-map {:type :ok})

(def default-error-map {:type :error :msg "An unknown error occured"})

(defn api-response
  [code map]
  {:status code
   :headers {"Content-Type" "application/json"}
   :body (merge default-response-map map)})

(defn error-response
  [code map]
  (api-response code (merge default-error-map map)))

(defn meta-params
  "Given a hashmap of query parameters grab the ones prefixed meta-, strip that off, and turn into a URI"
  [query-params]
  (reduce (fn [acc [k v]] (assoc acc
                                 (str "http://publishmydata.com/def/drafter/meta/"
                                      (subs k (+ 1 (.indexOf k "-")) (.length k)))
                                 v))
          {}
          (select-keys query-params
                       (filter (fn[p] (.startsWith p "meta-")) (keys query-params)))))
