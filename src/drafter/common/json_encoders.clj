(ns drafter.common.json-encoders
  (:require [cheshire.core :as ch]
            [cheshire.generate :refer [add-encoder encode-map]]
            [drafter.util :as util]))

(defn- exception-map [cause-map ex]
  (let [m {:message (.getMessage ex)
           :class (str (class ex))
           :stack-trace (mapv str (.getStackTrace ex))}]
    (if cause-map
      (assoc m :cause cause-map)
      m)))

(defn exception->map
  "Converts a Java exception into a map representation containing the
  following keys:
  :message - The exception message
  :class   - The full Java class name of the exception
  :stack-trace - Vector containing the string representation of each
                 stack frame
  :cause - Map for the cause of this exception if one exists"
  [ex]
  (let [causes (util/get-causes ex)]
    (reduce exception-map nil (reverse causes))))

(defn encode-exception
  "Encode an Exception instance to a JSON string"
  [^Exception ex gen]
  (let [ex-map (exception->map ex)]
    (encode-map ex-map gen)))

(defn register-custom-encoders!
  "Registers JSON encoders for types which may need to be serialised."
  []
  (add-encoder java.lang.Exception encode-exception))
