(ns drafter-client.client.impl)

(deftype DrafterClient [martian jws-key batch-size]
  ;; Wrap martian in a type so that:
  ;; a) we don't leak the jws-key
  ;; b) we don't expose the martian impl to the "system"
  ;; We can still get at the pieces if necessary due to the ILookup impl.
  clojure.lang.ILookup
  (valAt [this k] (.valAt this k nil))
  (valAt [this k default]
    (case k
      :martian martian
      :jws-key jws-key
      :batch-size batch-size
      (.valAt martian k default))))

(defn intercept [{:keys [martian jws-key batch-size] :as client} & interceptors]
  (->DrafterClient (apply update martian :interceptors conj interceptors)
                   jws-key
                   batch-size))
