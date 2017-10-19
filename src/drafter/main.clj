(ns drafter.main
  (:require [integrant.core :as ig]
            [aero.core :as aero]
            [clojure.java.io :as io]
            [cognician.dogstatsd :as datadog]))


(def system)

(defmethod ig/init-key :drafter.main/datadog [k {:keys [statsd-address tags]}]
  (datadog/configure! statsd-address {:tags tags}))

(defn- load-system
  [system-config]
  (-> system-config
                io/resource
                aero/read-config
                ig/init))

(defn start-system! [system-config]
  (let [cfg (load-system system-config)]
    (alter-var-root #'system (constantly cfg))))

(defn add-shutdown-hook! []
  (.addShutdownHook (Runtime/getRuntime) (Thread. stop-system!)))

(defn stop-system! []
  (ig/halt! system))

;; TODO
(defn -main [args]
  (add-shutdown-hook!)
  (start-system! "system.edn"))
