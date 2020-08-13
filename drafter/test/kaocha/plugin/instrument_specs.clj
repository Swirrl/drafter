(ns kaocha.plugin.instrument-specs
  (:require [kaocha.plugin :refer [defplugin]]
            [clojure.spec.test.alpha :as st]))

(def spec-namespaces
  '[drafter.spec
    drafter.async.spec
    drafter.backend.spec
    drafter.draftset.spec
    drafter.endpoint.spec
    drafter.user.spec])

(defn- load-spec-namespaces! []
  (doseq [ns spec-namespaces]
    (require ns)))

(defplugin
  kaocha.plugin/instrument-specs
  (pre-run [test-plan]
    (load-spec-namespaces!)
    (st/instrument)
    test-plan)

  (post-run [result]
    (st/unstrument)
    result))
