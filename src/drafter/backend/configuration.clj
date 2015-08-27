(ns drafter.backend.configuration
  (:require [clojure.tools.logging :as log]
            [clojure.string :as string]
            [drafter.backend.sesame.native :as native]
            [drafter.backend.sesame-stardog :as stardog]))

(def ^:private backend-fns
  {:sesame-native native/get-native-backend
   :sesame-stardog stardog/get-stardog-backend})

(defn- backend-key->name [backend-key]
  (-> backend-key
      name
      string/upper-case
      (string/replace "-" "_")))

(defn- backend-name->key [backend-name]
  (-> backend-name
      (string/replace "_" "-")
      string/lower-case
      keyword))

(defn- get-backend-names []
  (map backend-key->name (keys backend-fns)))

(defn- get-backend-key [env-map]
  (if-let [backend-name (:drafter-backend env-map)]
    (let [backend-key (backend-name->key backend-name)]
      (if (contains? backend-fns backend-key)
        backend-key
        (throw (RuntimeException. (str "Unsupported backend: " backend-name)))))
    (let [backend-name-list (string/join ", " (get-backend-names))
          msg (str "No backend configured - using Sesame native. To configure a backend, export a "
                   "DRAFTER_BACKEND environment variable set to one of the following values: "
                   backend-name-list)]
      (log/warn msg)
      :sesame-native)))

(defn- get-backend-fn [env-map]
  (let [backend-key (get-backend-key env-map)]
    (log/info "Using backend " (backend-key->name backend-key))
    (backend-key backend-fns)))

(defn get-backend [env-map]
  (let [backend-fn (get-backend-fn env-map)]
    (backend-fn env-map)))
