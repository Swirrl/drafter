(ns drafter-client.test-util.db
  (:require [clojure.test :as t]
            [grafter-2.rdf4j.repository :as repo]
            [grafter-2.rdf.protocols :as pr]
            [clojure.java.io :as io]))

(defn- contains-triples-or-quads? [repo]
  (with-open [conn (repo/->connection repo)]
    (let [q (slurp (io/resource "ask_contains_unexpected_data.sparql"))]
      (repo/query conn q))))

(defn delete-test-data! [repo]
  (with-open [conn (repo/->connection repo)]
    (let [q (slurp (io/resource "delete_test_data.sparql"))]
      (pr/update! conn q))))

(defn drop-all! [repo]
  (pr/update! (repo/->connection repo) "DROP ALL ;"))

(defn assert-empty [repo]
  (t/is (not (contains-triples-or-quads? repo))
        (str "The Stardog instance you are testing against " repo
             " contains data already."
             "It might not be safe to run the unit tests against it.  "
             "Check its safe and clean it before running the tests."
             "Alternatively run the tests with the environment variable "
             "DISABLE_DRAFTER_CLEANING_PROTECTION=true set.")))
