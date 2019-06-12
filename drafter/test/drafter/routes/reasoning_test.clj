(ns ^:rest-api drafter.routes.reasoning-test
  (:require [clojure.java.io :as io]
            [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [drafter.feature.draftset.create-test :as create-test]
            [drafter.test-common :as tc :refer [deftest-system]]
            [drafter.user-test :refer [test-editor test-publisher]]
            [grafter-2.rdf.protocols :refer [add context]]
            [grafter-2.rdf4j.formats :as formats]
            [grafter-2.rdf4j.io :refer [rdf-writer statements]]
            [schema.test :refer [validate-schemas]]
            [swirrl-server.async.jobs :refer [finished-jobs]]
            [clojure.string :as string]
            [clojure-csv.core :as csv]
            [drafter.routes.common
             :refer [default-sparql-query live-query
                     draftset-query append-quads-to-draftset-through-api
                     publish-draftset]]
            [clojure.set :as set])
  (:import [java.io ByteArrayInputStream ByteArrayOutputStream]))

(use-fixtures :each (join-fixtures [validate-schemas]))

(def system-config "drafter/routes/sparql-test/reasoning-system.edn")

(def in-prefixes "
@prefix : <http://test.com/> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix draft: <http://publishmydata.com/graphs/drafter/draft/> .
@prefix drafter: <http://publishmydata.com/def/drafter/> .
@prefix draftset: <http://publishmydata.com/def/drafter/draftset/> .
@prefix tbox: <http://publishmydata.com/graphs/reasoning-tbox> .
")

(def q-prefixes "
PREFIX : <http://test.com/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX draft: <http://publishmydata.com/graphs/drafter/draft/>
PREFIX drafter: <http://publishmydata.com/def/drafter/>
PREFIX draftset: <http://publishmydata.com/def/drafter/draftset/>
PREFIX tbox: <http://publishmydata.com/graphs/reasoning-tbox>
")

(defn draftset-get-data
  [draftset & {:keys [union-with-live reasoning] :as kwargs}]
  (tc/with-identity test-publisher
    (-> default-sparql-query
        (assoc :uri (str draftset "/data"))
        (cond->
            union-with-live (assoc-in [:params :union-with-live] "true")
            reasoning (assoc-in [:query-params "reasoning"] true)))))

(defn new-draftset [api quadstr]
  (let [req (create-test/create-draftset-request test-publisher nil nil)
        draftset (-> req api :headers (get "Location"))
        rdfstr (str in-prefixes quadstr)
        quad-istream (java.io.ByteArrayInputStream. (.getBytes rdfstr "UTF-8"))
        quads (statements quad-istream :format :trig)
        process (fn [xs] (map #(string/split % #",") xs))]
    [(fn add! []
       (append-quads-to-draftset-through-api api test-publisher draftset quads))
     (fn publish! []
       (publish-draftset api test-publisher draftset))
     (fn query [q & kwargs]
       (let [request (apply draftset-query draftset (str q-prefixes q) :user test-publisher kwargs)]
         (-> request api :body io/reader line-seq rest process set)))
     (fn get-data [& kwargs]
       (let [request (apply draftset-get-data draftset kwargs)]
         (-> request api :body io/reader line-seq rest process set)))]))

(deftest-system live-sparql-reasoning-test
  [{endpoint :drafter.routes.sparql/live-sparql-query-route :as system}
   system-config]
  (letfn [(process [xs] (map #(string/split % #",") xs))
          (run-query [q & kwargs]
            (let [request (apply live-query (str q-prefixes q) kwargs)]
              (-> request endpoint :body io/reader line-seq rest process set)))]

    (testing "Reasoning in querys is passed through"
      (testing "Query without reasoning"
        (let [q "SELECT ?t ?name {
                   ?t a :Dog .
                   ?t a ?type .
                   ?type rdfs:label ?name
                 }"]
          (is (= #{}
                 (run-query q)))))

      (testing "Same query with reasoning returns via hierarchy"
        (let [q "SELECT ?t ?name {
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
        (let [q "SELECT ?t WHERE {
                   ?t a :Mammal .
                   ?t a ?type
                   FILTER NOT EXISTS {
                     ?type rdfs:label ?name
                   }
                 }"]
          (is (= #{["http://test.com/Stevie"]
                   ["http://test.com/Tigger"]}
                 (run-query q :reasoning true)))))

      (testing "Query Stevie's hierarchy"
        (let [q "SELECT ?class { :Stevie a ?class }"]
          (is (= #{["http://test.com/WestHighlandTerrier"]
                   ["http://test.com/Dog"]
                   ["http://test.com/Mammal"]
                   ["http://test.com/Animal"]
                   ["http://test.com/Lifeform"]
                   ["http://www.w3.org/2002/07/owl#Thing"]}
                 (run-query q :reasoning true))))))))


(deftest-system draftset-sparql-reasoning-test
  [{api :drafter.routes/draftsets-api :as system}
   system-config]
  (let [draft-1 "tbox: { :Whippet a rdfs:Class ; rdfs:subClassOf :Dog . }"
        [add-to-draft-1! publish-1! draftq-1] (new-draftset api draft-1)
        draft-2 ":PublicGraph {
                   :Whippet
                     rdfs:label \"Whippet\" ;
                     rdfs:comment \"A whippet.\".
                   :Snappy a :Whippet .
                 }"
        [add-to-draft-2! publish-2! draftq-2] (new-draftset api draft-2)
        stevie-hierarchy #{["http://test.com/Stevie" "Lifeform"]
                           ["http://test.com/Stevie" "Mammal"]
                           ["http://test.com/Stevie" "West Highland Terrier"]
                           ["http://test.com/Stevie" "Animal"]
                           ["http://test.com/Stevie" "Dog"]}
        snappy-hierarchy #{["http://test.com/Snappy" "Lifeform"]
                           ["http://test.com/Snappy" "Mammal"]
                           ["http://test.com/Snappy" "Whippet"]
                           ["http://test.com/Snappy" "Animal"]
                           ["http://test.com/Snappy" "Dog"]}
        dog->label-q "SELECT ?t ?name {
                          ?t a :Dog .
                          ?t a ?type .
                          ?type rdfs:label ?name
                      }"
        dog-no-label-q "SELECT ?t WHERE {
                          ?t a :Mammal .
                          ?t a ?type
                          FILTER NOT EXISTS {
                            ?type rdfs:label ?name
                          }
                        }"]

    (add-to-draft-1!)
    (publish-1!)
    (add-to-draft-2!)

    (testing "Reasoning in querys is passed through"
      (testing "Query without reasoning"
        (is (= #{}
               (draftq-2 dog->label-q))))

      (testing "Same query with reasoning returns via hierarchy"
        (is (= (set/union stevie-hierarchy snappy-hierarchy)
               (draftq-2 dog->label-q :reasoning true))))

      (testing "Same query with reasoning returns via hierarchy (union w/live)"
        (is (= (set/union stevie-hierarchy snappy-hierarchy)
               (draftq-2 dog->label-q :union-with-live true :reasoning true))))

      (testing "Only select entities without rdfs:label"
        (is (= #{["http://test.com/Tigger"]
                 ["http://test.com/Snappy"]
                 ["http://test.com/Stevie"]}
               (draftq-2 dog-no-label-q :reasoning true))))

      (testing "Only select entities without rdfs:label (union w/live)"
        (is (= #{["http://test.com/Snappy"]
                 ["http://test.com/Stevie"]
                 ["http://test.com/Tigger"]}
               (draftq-2 dog-no-label-q :union-with-live true :reasoning true)))))))

(deftest-system draftset-get-data-reasoning-test
  [{api :drafter.routes/draftsets-api :as system}
   system-config]
  (let [draft-1 "tbox: { :Whippet a rdfs:Class ; rdfs:subClassOf :Dog . }"
        [add-to-draft-1! publish-1! _ get-data-1] (new-draftset api draft-1)
        draft-2 ":PublicGraph {
                   :Whippet
                     rdfs:label \"Whippet\" ;
                     rdfs:comment \"A whippet.\".
                   :Snappy a :Whippet .
                 }"
        [add-to-draft-2! publish-2! _ get-data-2] (new-draftset api draft-2)
        info #{["http://test.com/Lifeform" "http://www.w3.org/2000/01/rdf-schema#label" "Lifeform"]
               ["http://test.com/Lifeform" "http://www.w3.org/2000/01/rdf-schema#comment" "A living thing."]
               ["http://test.com/Animal" "http://www.w3.org/2000/01/rdf-schema#comment" "An animal."]
               ["http://test.com/Animal" "http://www.w3.org/2000/01/rdf-schema#label" "Animal"]
               ["http://test.com/Mammal" "http://www.w3.org/2000/01/rdf-schema#label" "Mammal"]
               ["http://test.com/Mammal" "http://www.w3.org/2000/01/rdf-schema#comment" "A mammal."]
               ["http://test.com/Dog" "http://www.w3.org/2000/01/rdf-schema#comment" "A dog."]
               ["http://test.com/Dog" "http://www.w3.org/2000/01/rdf-schema#label" "Dog"]
               ["http://test.com/Whippet" "http://www.w3.org/2000/01/rdf-schema#label" "Whippet"]
               ["http://test.com/Whippet" "http://www.w3.org/2000/01/rdf-schema#comment" "A whippet."]
               ["http://test.com/Snappy" "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" "http://test.com/Whippet"]
               ["http://test.com/WestHighlandTerrier" "http://www.w3.org/2000/01/rdf-schema#comment" "A west highland terrier."]
               ["http://test.com/WestHighlandTerrier" "http://www.w3.org/2000/01/rdf-schema#label" "West Highland Terrier"]
               ["http://test.com/Stevie" "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" "http://test.com/WestHighlandTerrier"]
               ["http://test.com/Cat" "http://www.w3.org/2000/01/rdf-schema#label" "Cat"]
               ["http://test.com/Cat" "http://www.w3.org/2000/01/rdf-schema#comment" "A cat."]
               ["http://test.com/Tiger" "http://www.w3.org/2000/01/rdf-schema#label" "Tiger"]
               ["http://test.com/Tiger" "http://www.w3.org/2000/01/rdf-schema#comment" "A tiger."]
               ["http://test.com/Tigger" "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" "http://test.com/Tiger"]}
        hierarchy #{["http://test.com/Lifeform" "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" "http://www.w3.org/2000/01/rdf-schema#Class"]
                    ["http://test.com/Animal" "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" "http://www.w3.org/2000/01/rdf-schema#Class"]
                    ["http://test.com/Animal" "http://www.w3.org/2000/01/rdf-schema#subClassOf" "http://test.com/Lifeform"]
                    ["http://test.com/Mammal" "http://www.w3.org/2000/01/rdf-schema#subClassOf" "http://test.com/Animal"]
                    ["http://test.com/Mammal" "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" "http://www.w3.org/2000/01/rdf-schema#Class"]
                    ["http://test.com/Dog" "http://www.w3.org/2000/01/rdf-schema#subClassOf" "http://test.com/Mammal"]
                    ["http://test.com/Dog" "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" "http://www.w3.org/2000/01/rdf-schema#Class"]
                    ["http://test.com/Whippet" "http://www.w3.org/2000/01/rdf-schema#subClassOf" "http://test.com/Dog"]
                    ["http://test.com/Whippet" "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" "http://www.w3.org/2000/01/rdf-schema#Class"]
                    ["http://test.com/WestHighlandTerrier" "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" "http://www.w3.org/2000/01/rdf-schema#Class"]
                    ["http://test.com/WestHighlandTerrier" "http://www.w3.org/2000/01/rdf-schema#subClassOf" "http://test.com/Dog"]
                    ["http://test.com/Cat" "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" "http://www.w3.org/2000/01/rdf-schema#Class"]
                    ["http://test.com/Cat" "http://www.w3.org/2000/01/rdf-schema#subClassOf" "http://test.com/Mammal"]
                    ["http://test.com/Tiger" "http://www.w3.org/2000/01/rdf-schema#subClassOf" "http://test.com/Cat"]
                    ["http://test.com/Tiger" "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" "http://www.w3.org/2000/01/rdf-schema#Class"]}]

    (add-to-draft-1!)
    (publish-1!)
    (add-to-draft-2!)

    (testing "Reasoning in get graph data is passed through"
      (testing "Graph data without reasoning"
        (is (= info
               (get-data-2))))

      (testing "Graph data with reasoning"
        (is (= info
               (get-data-2 :reasoning true))))

      (testing "Graph data without reasoning (union w/live)"
        (is (= (set/union info hierarchy)
               (get-data-2 :union-with-live true))))

      (testing "Graph data with reasoning (union w/live)"
        (is (= (set/union info hierarchy)
               (get-data-2 :union-with-live true :reasoning true)))))))

(deftest-system draftset-non-leaky-tbox-test
  [{live :drafter.routes.sparql/live-sparql-query-route
    api :drafter.routes/draftsets-api :as system}
   system-config]
  (letfn [(process [xs] (map #(string/split % #",") xs))
          (liveq [q & kwargs]
            (let [request (apply live-query (str q-prefixes q) kwargs)]
              (-> request live :body io/reader line-seq rest process set)))]

    (let [draft-1 ":PublicGraph { :Nickie a :WestHighlandTerrier . }"
          [add-to-draft-1! publish-1! draftq-1] (new-draftset api draft-1)
          nickie-hierarchy #{["http://test.com/WestHighlandTerrier"]
                             ["http://test.com/Dog"]
                             ["http://test.com/Animal"]
                             ["http://test.com/Mammal"]
                             ["http://test.com/Lifeform"]
                             ["http://www.w3.org/2002/07/owl#Thing"]}]

      (testing "What users can see before adding quads to draft TBOX"
        (let [q "SELECT ?class { :Nickie a ?class } "]
          (is (= #{["http://www.w3.org/2002/07/owl#Thing"]}
                 (liveq q :reasoning true)))
          (is (= #{}
                 (draftq-1 q :reasoning true)))
          (is (= #{["http://www.w3.org/2002/07/owl#Thing"]}
                 (draftq-1 q :reasoning true :union-with-live true)))))

      (add-to-draft-1!)

      (testing "What users can see after adding quads to draft TBOX"
        (let [q "SELECT ?class { :Nickie a ?class } "]
          (is (= #{["http://www.w3.org/2002/07/owl#Thing"]}
                 (liveq q :reasoning true)))
          (is (= nickie-hierarchy
                 (draftq-1 q :reasoning true)))
          (is (= nickie-hierarchy
                 (draftq-1 q :reasoning true :union-with-live true)))))

      (publish-1!)

      (let [draft-2 "tbox: { :WestHighlandTerrier a rdfs:Class; rdfs:subClassOf :RodentHunter . }"
            [add-to-draft-2! publish-2! draftq-2] (new-draftset api draft-2)]

        (add-to-draft-2!)

        (testing "What users can see after adding quads to draft TBOX"
          ;; ["http://test.com/RodentHunter"]
          ;; ^^ should not be visible in the hierarchy yet because the graph
          ;; this has been added to is not actually the TBOX, it's a drafter
          ;; temporary uuid graph.
          (let [q "SELECT ?class { :Nickie a ?class }"]
            (is (= nickie-hierarchy
                   (liveq q :reasoning true)))
            (is (= #{["http://www.w3.org/2002/07/owl#Thing"]}
                   (draftq-2 q :reasoning true)))
            ;; ^^ This draft has no :Nickie, so nothing returns, other than the
            ;; "Everything is a thing" owl:Thing.
            (is (= nickie-hierarchy
                   (draftq-2 q :reasoning true :union-with-live true)))))

        (publish-2!)

        (testing "What users can see after publishing quads to TBOX"
          ;; ["http://test.com/RodentHunter"]
          ;; ^^ should be visible now in the hierarchy because it's been added
          ;; to the real TBOX graph.
          (let [q "SELECT ?class { :Nickie a ?class }"]
            (is (= (conj nickie-hierarchy ["http://test.com/RodentHunter"])
                   (liveq q :reasoning true)))))))))
