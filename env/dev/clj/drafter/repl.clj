(ns drafter.repl
  (:require [clojure.java.io :as io]
            [clojure.pprint :as pprint]
            [clojure.set :as set]
            [clojure.spec.alpha :as s]
            [clojure.spec.test.alpha :as st]
            [drafter.main :as main :refer [system]]
            [eftest.runner :as eftest]
            [grafter-2.rdf4j.repository :as repo]
            [integrant.core :as ig]))

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
   (load-system (main/read-system (io/resource "system.edn")))

   (check-specs* (if (string? num-tests)
                   (Integer/parseInt num-tests)
                   num-tests))))

(defn stub-fdefs [set-of-syms]
  (st/instrument set-of-syms {:stub set-of-syms}))

(defn start-system!
  ([] (start-system! {:instrument? true}))
  ([{:keys [instrument?] :as opts}]

   (main/start-system!)
   (when instrument?
     (st/instrument))))

(defn run-tests []
  (eftest/run-tests (eftest/find-tests "test") {:multithread? false}))

(do (println)
    (println "   ___           _____         ")
    (println "  / _ \\_______ _/ _/ /____ ____")
    (println " / // / __/ _ `/ _/ __/ -_) __/")
    (println "/____/_/  \\_,_/_/ \\__/\\__/_/   ")
    (println)
    (println "Welcome to the Drafter REPL!")
    (println)
    (println)
    (println "REPL Commands: ")
    (println)
    (println "(start-system!)")
    (println "(stop-system!)")
    (println "(run-tests)"))

(comment

  (do
    (require '[grafter.core :as pr])
    (require '[grafter-2.rdf4j.repository :as repo])

    (def repo (:drafter.backend/rdf4j-repo system))
    (def conn (repo/->connection repo))



    (repo/query conn "PREFIX : <http://example> \n select * where { ?s ?p ?o } limit 10"))


  )
