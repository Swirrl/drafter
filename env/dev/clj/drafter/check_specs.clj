(ns drafter.check-specs
  (:require [clojure.java.io :as io]
            [clojure.pprint :as pprint]
            [integrant.core :as ig]
            [clojure.set :as set]
            [clojure.spec.alpha :as s]
            [clojure.spec.test.alpha :as st]
            [drafter.main :as main :refer [system]]))

(def disabled-specs
  "Specs listed here will be excluded from check-specs call.  Useful
  if we're still working on them and expect them to fail."
  #{`drafter.stasher/generate-drafter-cache-key})

(defn check-specs*
  ([] (check-specs* 20))
  ([num-tests]
   ;;(st/instrument)

   (let [fdefs-to-check (set/difference (st/checkable-syms)
                                        disabled-specs)
         _ (println "Checking specs " fdefs-to-check)
         results (st/check fdefs-to-check
                           {:clojure.spec.test.check/opts {:num-tests num-tests
                                                           ;; lower sizes of values created to be more memory efficient.
                                                           :max-size 50}})
         failures (remove (comp true? :result :clojure.spec.test.check/ret) results)]
     (pprint/print-table
       [:success? :symbol :seed]
       (for [result results]
         {:success? (true? (get-in result [:clojure.spec.test.check/ret :result]))
          :symbol (get result :sym)
          :seed (get-in result [:clojure.spec.test.check/ret :seed])}))
     (if (seq failures)
       (do
         (doseq [f failures]
           (println)
           (println "Spec Error for failing fdef: " (:sym f))
           (println)
           (pprint/pprint (-> f :clojure.spec.test.check/ret)))
         :some-spec-failures)
       :all-specs-pass))))

(defn load-system [system-config]
  (let [start-keys (keys system-config)]
    (ig/load-namespaces system-config start-keys)))

(defn check-specs
  ([num-tests command-line?]
   (let [res (check-specs num-tests)
         exit-code (cond
                     (= :all-specs-pass res) 0
                     (= :some-spec-failures res) 111 ;; distinguish from error code 1 which can be used by travis
                     :else 2)]
     (when command-line?
       (System/exit exit-code))))
  ([num-tests]

   (s/check-asserts true)
   (load-system (main/read-system (io/resource "drafter-base-config.edn")))

   (check-specs* (if (string? num-tests)
                   (Integer/parseInt num-tests)
                   num-tests))))
