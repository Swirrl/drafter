(ns drafter.initialise
  (:require [drafter.main :as main]
            [integrant.core :as ig]
            [clojure.edn :as edn]
            [clojure.spec.alpha :as s]))

(defn run-keys
  "Builds an integrant configuration from the specified configuration files
   then starts and stops the specified keys"
  [keys configs]
  (let [config (main/build-config configs)]
    (ig/load-namespaces config)
    (let [system (ig/init config keys)]
      (ig/halt! system)))
  nil)

(defn usage []
  (binding [*out* *err*]
    (printf "Usage: %s keys configs ...%n" (.getName (.ns #'usage)))
    (println)
    (println "Merges the specified integrant configuration files into the base drafter config and starts the specified keys")
    (println "The keys to start must be a non-empty collection of valid integrant config keys")))

(defn- read-keys
  "Parses the keys to start from a string. Returns the collection of
  integrant keys or raises an exception if the key specification is
  invalid."
  [keys-str]
  (let [ks (edn/read-string keys-str)]
    (if (s/valid? (s/coll-of ig/valid-config-key? :min-count 1) ks)
      ks
      (let [msg (format "Cannot convert \"%s\" into key sequence%nKeys must be a non-empty collection of integrant config keys"
                        keys-str)]
        (throw (ex-info msg {:type :invalid-keys
                             :value keys-str}))))))

(defn -main [& args]
  (if (< (count args) 2)
    (do
      (usage)
      (System/exit 1))
    (try
      (let [[ks & configs] args
            ks (read-keys ks)]
        (run-keys ks configs))
      (catch Exception ex
        (binding [*out* *err*]
          (println (.getMessage ex))
          (.printStackTrace ex)
          (System/exit 1))))))
