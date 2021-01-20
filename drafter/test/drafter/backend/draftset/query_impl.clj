(ns drafter.backend.draftset.query-impl
  (:require [clojure.test :as t]
            [drafter.rdf.sesame :as ses]
            [grafter-2.rdf4j.io :as gio]
            [clojure.test :as t]
            [drafter.rdf.dataset :as dataset])
  (:import [org.eclipse.rdf4j.query Operation]
           [java.net URI]))

(defn- test-bindings
  "Tests the get/set/remove/clear bindings methods work as expected for the given query. query-vars
   should be a sequence of query variable names within the query. These will be randomly bound and
   unbound to check the update operations work as expected."
  [^Operation query query-vars]
  {:pre [(seq query-vars)]}
  (letfn [(get-bindings-map []
            (ses/binding-set->map (.getBindings query)))]
    (let [to-add (into {} (map (fn [v] [v (gio/->rdf4j-uri (str "http://var-" v))]) query-vars))
          [to-remove to-keep] (split-at (rand-int (count query-vars)) query-vars)]
      (t/testing ".setBinding"
        (doseq [[query-var value] to-add]
                   (.setBinding query query-var value))
        (t/is (= to-add (get-bindings-map))))

      (t/testing ".removeBinding"
        (doseq [binding-name to-remove]
                   (.removeBinding query binding-name))
        (let [expected (select-keys to-add to-keep)]
          (t/is (= expected (get-bindings-map)))))

      (t/testing ".clearBindings"
        (.clearBindings query)
        (t/is (= {} (ses/binding-set->map (.getBindings query))) "Bindings remain after clear")))))

(defn test-operation-methods
  "Tests all the methods defined by the RDF4j Operation interface work as expected for the given
   perpared query instance. query-vars should be a non-empty collection of the variables within
   the query"
  [^Operation query query-vars]

  (test-bindings query query-vars)

  (t/testing "dataset"
    (let [dataset (dataset/create :named-graphs [(URI. "http://named-graph")]
                                  :default-graphs [(URI. "http://default")])]
      (.setDataset query (dataset/->rdf4j-dataset dataset))
      (t/is (= dataset (dataset/->dataset (.getDataset query))) "Unexpected dataset")))

  (t/testing "includeInferred"
    (doseq [b [true false]]
      (.setIncludeInferred query b)
      (t/is (= b (.getIncludeInferred query)) "Unexpected value for inferred")))

  (t/testing "maxExecutionTime"
    (let [max-execution-time 100]
      (.setMaxExecutionTime query max-execution-time)
      (t/is (= max-execution-time (.getMaxExecutionTime query)) "Unexpected max execution time"))))

