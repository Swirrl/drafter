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
