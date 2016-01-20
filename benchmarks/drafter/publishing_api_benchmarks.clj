(ns ^{:doc
      "A namespace to help evaluate drafter performance either from a repl or
      via lein perforate." }

    drafter.publishing-api-benchmarks
  (:require [drafter.client :refer [->client ->raw-endpoint]]
            [drafter.repl :refer [start-server stop-server ->int]]
            [drafter.protocols :as dc]
            [grafter.rdf.protocols :refer [update!]]
            [grafter.rdf.repository :refer [query]]
            [grafter.rdf :refer [add statements]]
            [environ.core :refer [env]]
            [perforate.core :refer [defgoal defcase defcase*]
    :as bench]))

(def http-port (->int (get env :drafter-http-port "30011")))

;;(start-server http-port)

(def c (->client (str "http://localhost:" http-port)))

(comment (defn drop-all []
           (update! (->raw-endpoint c) "DROP ALL ;")))

(defn drop-all []
  (println "tear down"))

(defn- read-quads [number-of-quads]
  (let [quads (into [] (->> (statements "./data/307mb.nt")
                            (take number-of-quads)))]
    quads))

(defgoal load-into-draftset "Benchmark loading data into a draftset.")

(comment (defcase* load-into-draftset :10k-quads
          (fn []
            (let [quads (read-quads 10 ;;1e4
                                    )]
              [(fn []
                 (-> (dc/create-draftset! c)
                     (add "http://load-test/graph/100k-quads" quads)))
               drop-all]))))

(defcase* load-into-draftset :10k-quads
           (fn []
             (println "setup test")
             [(fn []
                (+ 2 2))
              drop-all]))

(defgoal publish-draftset "Benchmark publishing a draftset.")

(comment (defcase* publish-draftset :10k-quads
           (fn []
             (let [quads (read-quads 10 ;;1e4
                                     )
                   draftset (-> (dc/create-draftset! c)
                                (add "http://load-test/graph/100k-quads" quads))]
               [(fn []
                  @(dc/publish! draftset))
                drop-all]))))

(defcase* publish-draftset :10k-quads
           (fn []
             (println "setup test")
             [(fn []
                (+ 1 1))
              drop-all]))

(defn wrap-with-database [f]
  (println "setup fixture called")
  (f)
  (println "setup fixture finished"))

;;(stop-server)
