(ns drafter-client.test-util.db
  (:require [clojure.test :as t]
            [grafter.rdf.repository :as repo]
            [grafter.rdf.protocols :as pr]))



(defn- contains-triples-or-quads? [repo]
  (repo/query repo
              "ASK {
                 SELECT * WHERE {
                   {  ?s ?p ?o }
                   UNION {
                     GRAPH ?g { ?s ?p ?o }
                   }
                } LIMIT 1
              }"))

(defn drop-all! [repo]
  (grafter.rdf.protocols/update! repo "DROP ALL ;"))

(defn assert-empty [repo]
  (t/is (not (contains-triples-or-quads? repo))
        (str "The Stardog instance you are testing against " repo
             " contains data already."
             "It might not be safe to run the unit tests against it.  "
             "Check its safe and clean it before running the tests."
             "Alternatively run the tests with the environment variable "
             "DISABLE_DRAFTER_CLEANING_PROTECTION=true set.")))
