(ns drafter.spec
  (:require [clojure.spec.alpha :as s]
            [drafter.util :as util]
            [clojure.spec.gen.alpha :as gen])
  (:import [java.net URI]))

(s/def :drafter/URI #(instance? URI %))

(s/def :drafter/EmailAddress
  (s/with-gen util/validate-email-address
              (fn [] (gen/fmap (fn [[a b]] (str a "@" b ".com"))
                               (gen/tuple (gen/string-alphanumeric) (gen/string-alphanumeric))))))

(def spec-namespaces
  '[drafter.spec
    drafter.async.spec
    drafter.backend.spec
    drafter.draftset.spec
    drafter.endpoint.spec
    drafter.user.spec])

(defn load-spec-namespaces! []
  (doseq [ns spec-namespaces]
    (require ns)))

