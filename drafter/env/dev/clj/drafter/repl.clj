(ns drafter.repl
  (:require [clojure.pprint :as pprint]
            [clojure.spec.test.alpha :as st]
            [clojure.spec.alpha :as s]
            [drafter.check-specs :refer [check-specs]]
            [drafter.main :as main :refer [system stop-system!]]
            [kaocha.repl :as test]
            [grafter-2.rdf4j.repository :as repo]
            [integrant.core :as ig]
            [drafter.spec :refer [load-spec-namespaces!]]
            [clojure.java.io :as io]
            [meta-merge.core :as mm]
            [drafter.test-common :as tc] ;; for :mock profile
            [integrant.repl :refer [clear go halt prep init reset reset-all set-prep!]]))

(def dev-config
  {:mock  (io/resource "drafter-dev-config.edn")
   :auth0 (io/resource "drafter-dev-auth0.edn")
   :basic (io/resource "drafter-basic-dev-config.edn")})

(defn profiles [auth-type]
  [(io/resource "drafter-base-config.edn")
   (dev-config auth-type)
   (io/resource "drafter-local-config.edn")])

(defn stub-fdefs [set-of-syms]
  (st/instrument set-of-syms {:stub set-of-syms}))

(load-spec-namespaces!)

(defn prep-system-profile [profile]
  #(apply mm/meta-merge (->> (profiles profile)
                            (remove nil?)
                            (map main/read-system))))

(set-prep! (prep-system-profile :auth0))

(defn reset-and-instrument
  "Resets the integrant system, but also instruments the spec
  definitions."

  []

  (reset)

  (st/instrument))

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
    (println "(reset)                                  ;; reset drafter dev system (default is :auth0 dev config)")
    (println "(set-prep! (prep-system-profile :mock))  ;; for a mock auth0 system, then (reset)")
    (println "(set-prep! (prep-system-profile :basic)) ;; for a mock auth0 system, then (reset)")
    (println)
    (println "(test/run-all)")
    (println)
    (println "(check-specs 100)"))

(comment

  (do
    (require '[grafter.core :as pr])
    (require '[grafter-2.rdf4j.repository :as repo])

    (def repo (:drafter.backend/rdf4j-repo system))
    (def conn (repo/->connection repo))



    (repo/query conn "PREFIX : <http://example> \n select * where { ?s ?p ?o } limit 10"))


  )
