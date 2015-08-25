(ns drafter.backend.configuration
  (:require [drafter.backend.sesame-native :as native]))

(defn get-backend []
  (native/get-native-backend))
