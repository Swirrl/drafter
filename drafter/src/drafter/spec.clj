(ns drafter.spec
  (:require [clojure.spec.alpha :as s]
            [drafter.util :as util]
            [clojure.spec.gen.alpha :as gen]
            [integrant.core :as ig])
  (:import [java.net URI]))

(s/def :drafter/URI #(instance? URI %))

(s/def :drafter/EmailAddress
  (s/with-gen util/validate-email-address
              (fn [] (gen/fmap (fn [[a b]] (str a "@" b ".com"))
                               (gen/tuple (gen/string-alphanumeric) (gen/string-alphanumeric))))))

(s/def ::graphset
  (s/coll-of (s/or :uri uri?
                   :regex (partial instance? java.util.regex.Pattern))
             :kind set?))

(s/def :drafter/protected-graphs
  (s/keys :req-un [::graphset]))

(defmethod ig/init-key :drafter/protected-graphs [_ opts]
  ;; :groan: :duct/const
  opts)

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
