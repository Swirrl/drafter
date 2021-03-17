(ns drafter.stasher.cancellable
  (:refer-clojure :exclude [with-open]))

(defprotocol Cancellable
  (cancel [this]))

(defmacro with-open
  "Like clojure.core/with-open but calls cancel if there's an error."
  [bindings & body]
  (cond
    (= (count bindings) 0) `(do ~@body)
    (symbol? (bindings 0)) `(let ~(subvec bindings 0 2)
                              (try
                                (with-open ~(subvec bindings 2) ~@body)
                                (catch Throwable ex#
                                  (cancel ~(bindings 0))
                                  (throw ex#))
                                (finally
                                  (. ~(bindings 0) close))))
    :else (throw (IllegalArgumentException.
                  "with-open only allows Symbols in bindings"))))
