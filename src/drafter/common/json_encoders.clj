(ns drafter.common.json-encoders
  (:require [cheshire.core :as ch]
            [cheshire.generate :refer [add-encoder encode-map]]))

(defn encode-exception
  "Encode an Exception instance to a JSON string"
  [^Exception ex gen]
  (encode-map {:message (.getMessage ex)} gen))

(defn register-custom-encoders!
  "Registers JSON encoders for types which may need to be serialised."
  []
  (add-encoder java.lang.Exception encode-exception))
