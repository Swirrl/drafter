(ns drafter.routes.reasoning-test
  (:require [clojure.java.io :as io]
            [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [drafter.feature.draftset.create-test :as create-test]
            [drafter.test-common :as tc :refer [deftest-system]]
            [drafter.user-test :refer [test-editor]]
            [grafter.rdf :refer [add context statements]]
            [grafter.rdf4j.formats :as formats]
            [grafter.rdf4j.io :refer [rdf-writer]]
            [schema.test :refer [validate-schemas]]
            [swirrl-server.async.jobs :refer [finished-jobs]]
            [clojure.string :as string]
            [clojure-csv.core :as csv]
            [drafter.routes.common
             :refer [default-sparql-query live-query
                     draftset-query append-quads-to-draftset-through-api]])
  (:import [java.io ByteArrayInputStream ByteArrayOutputStream]))

(use-fixtures :each (join-fixtures [validate-schemas]))

(def system-config "drafter/routes/sparql-test/reasoning-system.edn")

(defn draftset-get-data
  [draftset & {:keys [union-with-live reasoning] :as kwargs}]
  (tc/with-identity test-editor
    (-> default-sparql-query
        (assoc :uri (str draftset "/data"))
        (cond->
            union-with-live (assoc-in [:params :union-with-live] "true")
            reasoning (assoc-in [:query-params "reasoning"] true)))))

(deftest-system live-sparql-reasoning-test
  [{endpoint :drafter.routes.sparql/live-sparql-query-route :as system}
   system-config]
  (letfn [(process [xs] (map #(string/split % #",") xs))
          (run-query [q & kwargs]
            (let [request (apply live-query q kwargs)]
              (-> request endpoint :body io/reader line-seq rest process set)))]

    (testing "Reasoning in querys is passed through"
      (testing "Query without reasoning"
        (let [q "PREFIX : <http://test.com/>
                 PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                 PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                 PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
                 SELECT ?t ?name
                 {
                     ?t a :Dog .
                     ?t a ?type .
                     ?type rdfs:label ?name
                 }"]
          (is (= #{}
                 (run-query q)))))

      (testing "Same query with reasoning returns via hierarchy"
        (let [q "PREFIX : <http://test.com/>
                 PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                 PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                 PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
                 SELECT ?t ?name
                 {
                     ?t a :Dog .
                     ?t a ?type .
                     ?type rdfs:label ?name
                 }"]
          (is (= #{["http://test.com/Stevie" "Lifeform"]
                   ["http://test.com/Stevie" "Mammal"]
                   ["http://test.com/Stevie" "West Highland Terrier"]
                   ["http://test.com/Stevie" "Animal"]
                   ["http://test.com/Stevie" "Dog"]}
                 (run-query q :reasoning true)))))

      (testing "Only select entities without rdfs:label"
        (let [q "PREFIX : <http://test.com/>
                 PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                 PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                 PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
                 SELECT ?t WHERE {
                   ?t a :Mammal .
                   ?t a ?type
                   FILTER NOT EXISTS {
                     ?type rdfs:label ?name
                   }
                 }"]
          (is (= #{["http://test.com/Stevie"]
                   ["http://test.com/Tigger"]}
                 (run-query q :reasoning true))))))))

(deftest-system draftset-sparql-reasoning-test
  [{api :drafter.routes/draftsets-api :as system}
   system-config]
  (let [req (create-test/create-draftset-request test-editor nil nil)
        draftset (-> req api :headers (get "Location"))
        quads (statements "test/resources/drafter/routes/sparql-test/reasoning-additions.trig")]
    (letfn [(process [xs] (map #(string/split % #",") xs))
            (run-query [q & kwargs]
              (let [request (apply draftset-query draftset q kwargs)]
                (-> request api :body io/reader line-seq rest process set)))]

      (append-quads-to-draftset-through-api api test-editor draftset quads)

      (testing "Reasoning in querys is passed through"
        (testing "Query without reasoning"
          (let [q "PREFIX : <http://test.com/>
                   PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                   PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                   PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
                   SELECT ?t ?name
                   {
                       ?t a :Dog .
                       ?t a ?type .
                       ?type rdfs:label ?name
                   }"]
            (is (= #{}
                   (run-query q)))))

        (testing "Same query with reasoning returns via hierarchy"
          (let [q "PREFIX : <http://test.com/>
                   PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                   PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                   PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
                   SELECT ?t ?name
                   {
                       ?t a :Dog .
                       ?t a ?type .
                       ?type rdfs:label ?name
                   }"]
            (is (= #{["http://test.com/Snappy" "Whippet"]}
                   (run-query q :reasoning true)))))

        (testing "Same query with reasoning returns via hierarchy (union w/live)"
          (let [q "PREFIX : <http://test.com/>
                   PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                   PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                   PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
                   SELECT ?t ?name
                   {
                       ?t a :Dog .
                       ?t a ?type .
                       ?type rdfs:label ?name
                   }"]
            (is (= #{["http://test.com/Stevie" "Lifeform"]
                     ["http://test.com/Stevie" "Mammal"]
                     ["http://test.com/Stevie" "West Highland Terrier"]
                     ["http://test.com/Stevie" "Animal"]
                     ["http://test.com/Stevie" "Dog"]
                     ["http://test.com/Snappy" "Lifeform"]
                     ["http://test.com/Snappy" "Mammal"]
                     ["http://test.com/Snappy" "Whippet"]
                     ["http://test.com/Snappy" "Animal"]
                     ["http://test.com/Snappy" "Dog"]}
                   (run-query q :union-with-live true :reasoning true)))))

        (testing "Only select entities without rdfs:label"
          (let [q "PREFIX : <http://test.com/>
                   PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                   PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                   PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
                   SELECT ?t WHERE {
                     ?t a :Mammal .
                     ?t a ?type
                     FILTER NOT EXISTS {
                       ?type rdfs:label ?name
                     }
                   }"]
            (is (= #{["http://test.com/Snappy"]}
                   (run-query q :reasoning true)))))

        (testing "Only select entities without rdfs:label (union w/live)"
          (let [q "PREFIX : <http://test.com/>
                   PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                   PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                   PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
                   SELECT ?t WHERE {
                     ?t a :Mammal .
                     ?t a ?type
                     FILTER NOT EXISTS {
                       ?type rdfs:label ?name
                     }
                   }"]
            (is (= #{["http://test.com/Snappy"]
                     ["http://test.com/Stevie"]
                     ["http://test.com/Tigger"]}
                   (run-query q :union-with-live true :reasoning true)))))))))

(deftest-system draftset-get-data-reasoning-test
  [{api :drafter.routes/draftsets-api :as system}
   system-config]
  (let [req (create-test/create-draftset-request test-editor nil nil)
        draftset (-> req api :headers (get "Location"))
        quads (statements "test/resources/drafter/routes/sparql-test/reasoning-additions.trig")]
    (letfn [(process [xs] (map #(string/split % #",") xs))
            (get-data [& kwargs]
              (let [request (apply draftset-get-data draftset kwargs)]
                (-> request api :body io/reader line-seq rest process set)))]

      (append-quads-to-draftset-through-api api test-editor draftset quads)

      (testing "Reasoning in get graph data is passed through"
        (testing "Graph data without reasoning"
          (is (= #{["http://test.com/Snappy" "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" "http://test.com/Whippet"]
                   ["http://test.com/Whippet" "http://www.w3.org/2000/01/rdf-schema#label" "Whippet"]
                   ["http://test.com/Whippet" "http://www.w3.org/2000/01/rdf-schema#subClassOf" "http://test.com/Dog"]
                   ["http://test.com/Whippet" "http://www.w3.org/2000/01/rdf-schema#comment" "A whippet."]
                   ["http://test.com/Whippet" "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" "http://www.w3.org/2000/01/rdf-schema#Class"]}
                 (get-data))))

        (testing "Graph data with reasoning"
          (is (= #{["http://test.com/Snappy" "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" "http://test.com/Whippet"]
                   ["http://test.com/Whippet" "http://www.w3.org/2000/01/rdf-schema#label" "Whippet"]
                   ["http://test.com/Whippet" "http://www.w3.org/2000/01/rdf-schema#subClassOf" "http://test.com/Dog"]
                   ["http://test.com/Whippet" "http://www.w3.org/2000/01/rdf-schema#comment" "A whippet."]
                   ["http://test.com/Whippet" "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" "http://www.w3.org/2000/01/rdf-schema#Class"]}
                 (get-data :reasoning true))))

        (testing "Graph data without reasoning (union w/live)"
          (is (= #{["http://test.com/Mammal" "http://www.w3.org/2000/01/rdf-schema#label" "Mammal"]
                   ["http://test.com/Snappy" "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" "http://test.com/Whippet"]
                   ["http://test.com/Whippet" "http://www.w3.org/2000/01/rdf-schema#label" "Whippet"]
                   ["http://test.com/Animal" "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" "http://www.w3.org/2000/01/rdf-schema#Class"]
                   ["http://test.com/Dog" "http://www.w3.org/2000/01/rdf-schema#comment" "A dog."]
                   ["http://test.com/Lifeform" "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" "http://www.w3.org/2000/01/rdf-schema#Class"]
                   ["http://test.com/Whippet" "http://www.w3.org/2000/01/rdf-schema#subClassOf" "http://test.com/Dog"]
                   ["http://test.com/Tiger" "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" "http://www.w3.org/2000/01/rdf-schema#Class"]
                   ["http://test.com/Mammal" "http://www.w3.org/2000/01/rdf-schema#comment" "A mammal."]
                   ["http://test.com/Tigger" "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" "http://test.com/Tiger"]
                   ["http://test.com/Dog" "http://www.w3.org/2000/01/rdf-schema#label" "Dog"]
                   ["http://test.com/WestHighlandTerrier" "http://www.w3.org/2000/01/rdf-schema#label" "West Highland Terrier"]
                   ["http://test.com/Whippet" "http://www.w3.org/2000/01/rdf-schema#comment" "A whippet."]
                   ["http://test.com/Dog" "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" "http://www.w3.org/2000/01/rdf-schema#Class"]
                   ["http://test.com/Cat" "http://www.w3.org/2000/01/rdf-schema#label" "Cat"]
                   ["http://test.com/Animal" "http://www.w3.org/2000/01/rdf-schema#comment" "An animal."]
                   ["http://test.com/Lifeform" "http://www.w3.org/2000/01/rdf-schema#comment" "A living thing."]
                   ["http://test.com/Cat" "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" "http://www.w3.org/2000/01/rdf-schema#Class"]
                   ["http://test.com/Mammal" "http://www.w3.org/2000/01/rdf-schema#subClassOf" "http://test.com/Animal"]
                   ["http://test.com/Tiger" "http://www.w3.org/2000/01/rdf-schema#subClassOf" "http://test.com/Cat"]
                   ["http://test.com/Whippet" "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" "http://www.w3.org/2000/01/rdf-schema#Class"]
                   ["http://test.com/WestHighlandTerrier" "http://www.w3.org/2000/01/rdf-schema#subClassOf" "http://test.com/Dog"]
                   ["http://test.com/Animal" "http://www.w3.org/2000/01/rdf-schema#subClassOf" "http://test.com/Lifeform"]
                   ["http://test.com/Stevie" "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" "http://test.com/WestHighlandTerrier"]
                   ["http://test.com/Tiger" "http://www.w3.org/2000/01/rdf-schema#label" "Tiger"]
                   ["http://test.com/Tiger" "http://www.w3.org/2000/01/rdf-schema#comment" "A tiger."]
                   ["http://test.com/Cat" "http://www.w3.org/2000/01/rdf-schema#comment" "A cat."]
                   ["http://test.com/Mammal" "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" "http://www.w3.org/2000/01/rdf-schema#Class"]
                   ["http://test.com/Dog" "http://www.w3.org/2000/01/rdf-schema#subClassOf" "http://test.com/Mammal"]
                   ["http://test.com/WestHighlandTerrier" "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" "http://www.w3.org/2000/01/rdf-schema#Class"]
                   ["http://test.com/Lifeform" "http://www.w3.org/2000/01/rdf-schema#label" "Lifeform"]
                   ["http://test.com/Animal" "http://www.w3.org/2000/01/rdf-schema#label" "Animal"]
                   ["http://test.com/Cat" "http://www.w3.org/2000/01/rdf-schema#subClassOf" "http://test.com/Mammal"]
                   ["http://test.com/WestHighlandTerrier" "http://www.w3.org/2000/01/rdf-schema#comment" "A west highland terrier."]}
                 (get-data :union-with-live true))))

        (testing "Graph data with reasoning (union w/live)"
          (is (= #{["http://test.com/Mammal" "http://www.w3.org/2000/01/rdf-schema#label" "Mammal"]
                   ["http://test.com/Snappy" "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" "http://test.com/Whippet"]
                   ["http://test.com/Whippet" "http://www.w3.org/2000/01/rdf-schema#label" "Whippet"]
                   ["http://test.com/Animal" "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" "http://www.w3.org/2000/01/rdf-schema#Class"]
                   ["http://test.com/Dog" "http://www.w3.org/2000/01/rdf-schema#comment" "A dog."]
                   ["http://test.com/Lifeform" "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" "http://www.w3.org/2000/01/rdf-schema#Class"]
                   ["http://test.com/Whippet" "http://www.w3.org/2000/01/rdf-schema#subClassOf" "http://test.com/Dog"]
                   ["http://test.com/Tiger" "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" "http://www.w3.org/2000/01/rdf-schema#Class"]
                   ["http://test.com/Mammal" "http://www.w3.org/2000/01/rdf-schema#comment" "A mammal."]
                   ["http://test.com/Tigger" "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" "http://test.com/Tiger"]
                   ["http://test.com/Dog" "http://www.w3.org/2000/01/rdf-schema#label" "Dog"]
                   ["http://test.com/WestHighlandTerrier" "http://www.w3.org/2000/01/rdf-schema#label" "West Highland Terrier"]
                   ["http://test.com/Whippet" "http://www.w3.org/2000/01/rdf-schema#comment" "A whippet."]
                   ["http://test.com/Dog" "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" "http://www.w3.org/2000/01/rdf-schema#Class"]
                   ["http://test.com/Cat" "http://www.w3.org/2000/01/rdf-schema#label" "Cat"]
                   ["http://test.com/Animal" "http://www.w3.org/2000/01/rdf-schema#comment" "An animal."]
                   ["http://test.com/Lifeform" "http://www.w3.org/2000/01/rdf-schema#comment" "A living thing."]
                   ["http://test.com/Cat" "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" "http://www.w3.org/2000/01/rdf-schema#Class"]
                   ["http://test.com/Mammal" "http://www.w3.org/2000/01/rdf-schema#subClassOf" "http://test.com/Animal"]
                   ["http://test.com/Tiger" "http://www.w3.org/2000/01/rdf-schema#subClassOf" "http://test.com/Cat"]
                   ["http://test.com/Whippet" "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" "http://www.w3.org/2000/01/rdf-schema#Class"]
                   ["http://test.com/WestHighlandTerrier" "http://www.w3.org/2000/01/rdf-schema#subClassOf" "http://test.com/Dog"]
                   ["http://test.com/Animal" "http://www.w3.org/2000/01/rdf-schema#subClassOf" "http://test.com/Lifeform"]
                   ["http://test.com/Stevie" "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" "http://test.com/WestHighlandTerrier"]
                   ["http://test.com/Tiger" "http://www.w3.org/2000/01/rdf-schema#label" "Tiger"]
                   ["http://test.com/Tiger" "http://www.w3.org/2000/01/rdf-schema#comment" "A tiger."]
                   ["http://test.com/Cat" "http://www.w3.org/2000/01/rdf-schema#comment" "A cat."]
                   ["http://test.com/Mammal" "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" "http://www.w3.org/2000/01/rdf-schema#Class"]
                   ["http://test.com/Dog" "http://www.w3.org/2000/01/rdf-schema#subClassOf" "http://test.com/Mammal"]
                   ["http://test.com/WestHighlandTerrier" "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" "http://www.w3.org/2000/01/rdf-schema#Class"]
                   ["http://test.com/Lifeform" "http://www.w3.org/2000/01/rdf-schema#label" "Lifeform"]
                   ["http://test.com/Animal" "http://www.w3.org/2000/01/rdf-schema#label" "Animal"]
                   ["http://test.com/Cat" "http://www.w3.org/2000/01/rdf-schema#subClassOf" "http://test.com/Mammal"]
                   ["http://test.com/WestHighlandTerrier" "http://www.w3.org/2000/01/rdf-schema#comment" "A west highland terrier."]}
                 (get-data :union-with-live true :reasoning true))))))))
