(ns kaocha.plugin.auth-env-plugin
  (:require [clojure.pprint]
            [kaocha.testable]
            [kaocha.plugin :refer [defplugin]]))

(def ^:dynamic *auth-env* :auth0)

(defplugin kaocha.plugin/auth-env-plugin
  (wrap-run [run test-plan]
    (fn [testable plan]
      (if-let [env (:auth-env testable)]
        (binding [*auth-env* env]
          (run testable plan))
        (run testable plan)))))
