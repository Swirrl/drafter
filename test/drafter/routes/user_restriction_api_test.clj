(ns drafter.routes.user-restriction-api-test
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
             :refer [live-query draftset-query append-quads-to-draftset-through-api]])
  (:import [java.io ByteArrayInputStream ByteArrayOutputStream]))

(use-fixtures :each (join-fixtures [validate-schemas]))

(def system-config "drafter/routes/sparql-test/graph-restriction-system.edn")

(defn check-user-restrictions [run-query]

  (testing "Public graphs are visible"
    (let [q "SELECT DISTINCT ?g WHERE { GRAPH ?g { ?s ?p ?o } }"]
      (is (= #{"http://test.com/graph-1"
               "http://test.com/graph-2"
               "http://test.com/graph-3"
               "http://test.com/fake-default-graph-1"
               "http://test.com/fake-default-graph-2"}
             (run-query q)))))

  (testing "User can select triples from public graphs"
    (let [q "SELECT DISTINCT ?s WHERE { ?s ?p ?o }"
          result (run-query q)]
      (is (contains? result "http://test.com/subject-1"))))

  (testing "User cannot select triples from private graphs"
    (let [q "SELECT DISTINCT ?s WHERE { ?s ?p ?o }"
          result (run-query q)]
      (is (not (contains? result "http://test.com/subject-4")))))

  (testing "Public / private restrictions in query\n"

    (testing "User can select public graphs in query"
      (let [q "SELECT DISTINCT ?g
                 FROM NAMED <http://test.com/graph-1>
                 FROM NAMED <http://test.com/graph-2>
                 WHERE { GRAPH ?g { ?s ?p ?o } }"]
        (is (= #{"http://test.com/graph-1"
                 "http://test.com/graph-2"}
               (run-query q)))))

    (testing "User can't select private graphs in query"
      (let [q "SELECT DISTINCT ?g
                 FROM NAMED <http://test.com/graph-4>
                 WHERE { GRAPH ?g { ?s ?p ?o } }"]
        (is (= #{}
               (run-query q)))))

    (testing "User can't select private graphs, but public graphs in same query are OK"
      (let [q "SELECT DISTINCT ?g
                 FROM NAMED <http://test.com/graph-2>
                 FROM NAMED <http://test.com/graph-4>
                 WHERE { GRAPH ?g { ?s ?p ?o } }"]
        (is (= #{"http://test.com/graph-2"}
               (run-query q))))))


  (testing "Public / private restrictions in protocol\n"

    (testing "User can select public graphs in query"
      (let [q "SELECT DISTINCT ?g WHERE { GRAPH ?g { ?s ?p ?o } }"]
        (is (= #{"http://test.com/graph-1"
                 "http://test.com/graph-2"}
               (run-query q :named-graph-uri ["http://test.com/graph-1"
                                              "http://test.com/graph-2"])))))

    (testing "User can't select private graphs in query"
      (let [q "SELECT DISTINCT ?g WHERE { GRAPH ?g { ?s ?p ?o } }"]
        (is (= #{}
               (run-query q :named-graph-uri ["http://test.com/graph-4"])))))

    (testing "User can't select private graphs, but public graphs in same query are OK"
      (let [q "SELECT DISTINCT ?g WHERE { GRAPH ?g { ?s ?p ?o } }"]
        (is (= #{"http://test.com/graph-2"}
               (run-query q
                          :named-graph-uri ["http://test.com/graph-2"
                                            "http://test.com/graph-4"])))))))

(deftest-system live-sparql-user-restrictions-test
  [{endpoint :drafter.routes.sparql/live-sparql-query-route
    :keys [drafter.stasher/repo] :as system}
   system-config]
  (letfn [(run-query [q & kwargs]
            (let [request (apply live-query q kwargs)]
              (-> request endpoint :body io/reader line-seq rest set)))]
    (check-user-restrictions run-query)))


(deftest-system draftset-sparql-user-restrictions-test
  [{endpoint :drafter.routes.draftsets-api/draftset-query-handler
    api :drafter.routes/draftsets-api
    :keys [drafter.stasher/repo] :as system}
   system-config]
  (let [req (create-test/create-draftset-request test-editor nil nil)
        draftset (-> req api :headers (get "Location"))
        quads (statements "test/resources/drafter/routes/sparql-test/graph-restriction-additions.trig")]
    (letfn [(run-query [q & kwargs]
              (let [{:keys [->csv?]} (apply hash-map kwargs)
                    request (apply draftset-query draftset q kwargs)
                    process (if ->csv?
                              (partial map csv/parse-csv)
                              identity)]
                (-> request api :body io/reader line-seq rest process set)))]

      (check-user-restrictions (fn [q & kwargs]
                                 (apply run-query q :union-with-live true kwargs)))

      (append-quads-to-draftset-through-api api test-editor draftset quads)

      (testing "Live (union) public graphs are visible"
        (let [q "SELECT DISTINCT ?g WHERE { GRAPH ?g { ?s ?p ?o } }"]
          (is (= #{"http://test.com/graph-1"
                   "http://test.com/graph-2"
                   "http://test.com/graph-3"
                   "http://test.com/graph-6"
                   "http://test.com/fake-default-graph-1"
                   "http://test.com/fake-default-graph-2"
                   "http://test.com/fake-default-graph-3"}
                 (run-query q :union-with-live true)))))

      (testing "User can select added triples"
        (let [q "SELECT ?s ?p ?o
                 WHERE { ?s ?p ?o }"]
          (is (=  #{
                    [["http://test.com/subject-1" "http://test.com/hasProperty" "http://test.com/data/2"]]
                    [["http://test.com/subject-1" "http://test.com/hasProperty" "http://test.com/data/3"]]
                    [["http://test.com/subject-1" "http://test.com/hasProperty" "http://test.com/data/1"]]

                    [["http://test.com/subject-6" "http://test.com/hasProperty" "http://test.com/data/2"]]
                    [["http://test.com/subject-6" "http://test.com/hasProperty" "http://test.com/data/1"]]

                    [["http://test.com/graph-6" "http://publisher" "http://freddy"]]

                    }
                  (run-query q :->csv? true)))))

      (testing "Restrictions in query"

        (testing "User can select added triples by graph"
          (let [q "SELECT ?s ?p ?o
                 FROM <http://test.com/graph-1>
                 WHERE { ?s ?p ?o }"]
            (is (= #{[["http://test.com/subject-1" "http://test.com/hasProperty" "http://test.com/data/2"]]
                     [["http://test.com/subject-1" "http://test.com/hasProperty" "http://test.com/data/3"]]
                     [["http://test.com/subject-1" "http://test.com/hasProperty" "http://test.com/data/1"]]}
                   (run-query q :->csv? true)))))

        (testing "User can select public (added) graphs"
          (let [q "SELECT DISTINCT ?g
                 FROM NAMED <http://test.com/graph-6>
                 WHERE { GRAPH ?g { ?s ?p ?o } }"]
            (is (= #{"http://test.com/graph-6"}
                   (run-query q)))))

        (testing "User can select public (added w/union) graphs"
          (let [q "SELECT DISTINCT ?g
                 FROM NAMED <http://test.com/graph-6>
                 WHERE { GRAPH ?g { ?s ?p ?o } }"]
            (is (= #{"http://test.com/graph-6"}
                   (run-query q :union-with-live true)))))

        (testing "User can select public graphs (added + union)"
          (let [q "SELECT DISTINCT ?g
                 FROM NAMED <http://test.com/graph-1>
                 FROM NAMED <http://test.com/graph-6>
                 WHERE { GRAPH ?g { ?s ?p ?o } }"]
            (is (= #{"http://test.com/graph-1"
                     "http://test.com/graph-6"}
                   (run-query q :union-with-live true)))))

        (testing "User can't select private graphs, but added and public graphs are OK"
          (let [q "SELECT DISTINCT ?g
                 FROM NAMED <http://test.com/graph-1>
                 FROM NAMED <http://test.com/graph-4>
                 FROM NAMED <http://test.com/graph-6>
                 WHERE { GRAPH ?g { ?s ?p ?o } }"]
            (is (= #{"http://test.com/graph-1"
                     "http://test.com/graph-6"}
                   (run-query q :union-with-live true))))))

      (testing "Restrictions in protocol"

        (testing "User can select added triples by graph"
          (let [q "SELECT ?s ?p ?o WHERE { ?s ?p ?o }"]
            (is (= #{[["http://test.com/subject-1" "http://test.com/hasProperty" "http://test.com/data/2"]]
                     [["http://test.com/subject-1" "http://test.com/hasProperty" "http://test.com/data/3"]]
                     [["http://test.com/subject-1" "http://test.com/hasProperty" "http://test.com/data/1"]]}
                   (run-query q :->csv? true :default-graph-uri ["http://test.com/graph-1"])))))

        (testing "User can select public (added) graphs"
          (let [q "SELECT DISTINCT ?g WHERE { GRAPH ?g { ?s ?p ?o } }"]
            (is (= #{"http://test.com/graph-6"}
                   (run-query q :named-graph-uri ["http://test.com/graph-6"])))))

        (testing "User can select public (added w/union) graphs"
          (let [q "SELECT DISTINCT ?g WHERE { GRAPH ?g { ?s ?p ?o } }"]
            (is (= #{"http://test.com/graph-6"}
                   (run-query q
                              :union-with-live true
                              :named-graph-uri ["http://test.com/graph-6"])))))

        (testing "User can select public graphs (added + union)"
          (let [q "SELECT DISTINCT ?g WHERE { GRAPH ?g { ?s ?p ?o } }"]
            (is (= #{"http://test.com/graph-1"
                     "http://test.com/graph-6"}
                   (run-query q
                              :union-with-live true
                              :named-graph-uri ["http://test.com/graph-1"
                                                "http://test.com/graph-6"])))))

        (testing "User can't select private graphs, but added and public graphs are OK"
          (let [q "SELECT DISTINCT ?g WHERE { GRAPH ?g { ?s ?p ?o } }"]
            (is (= #{"http://test.com/graph-1"
                     "http://test.com/graph-6"}
                   (run-query q
                              :union-with-live true
                              :named-graph-uri ["http://test.com/graph-1"
                                                "http://test.com/graph-4"
                                                "http://test.com/graph-6"])))))))))

(deftest-system live-from-and-from-named-interaction-test
  [{endpoint :drafter.routes.sparql/live-sparql-query-route
    :keys [drafter.stasher/repo] :as system}
   system-config]
  (letfn [(run-query [q & kwargs]
            (let [{:keys [->csv?]} (apply hash-map kwargs)
                  request (apply live-query q kwargs)
                  process (if ->csv? (partial map csv/parse-csv) identity)]
              (-> request endpoint :body io/reader line-seq rest process set)))]

    (testing "FROM and FROM NAMED in user query"
      (testing "FROM doesn't allow selection of named triples"
       (let [q "SELECT ?g ?s
                FROM <http://test.com/fake-default-graph-1>
                WHERE { GRAPH ?g { ?s ?p ?o } }"]
         (is (= #{}
                (run-query q)))))

      (testing "FROM NAMED doesn't allow selection from default graph"
        (let [q "SELECT ?g ?s
                 FROM NAMED <http://test.com/fake-default-graph-1>
                 WHERE { ?s ?p ?o }"]
          (is (= #{}
                 (run-query q)))))

      (testing "FROM allows selection of triples from only the specified graph(s)"
        (let [q "SELECT ?s ?p ?o
                 FROM <http://test.com/fake-default-graph-1>
                 WHERE { ?s ?p ?o }"]
          (is (= #{[["http://test.com/graph-1" "http://publisher" "http://freddy"]]}
                 (run-query q :->csv? true))))

        (let [q "SELECT ?s ?p ?o
                 FROM <http://test.com/fake-default-graph-1>
                 FROM <http://test.com/fake-default-graph-2>
                 WHERE { ?s ?p ?o }"]
          (is (= #{[["http://test.com/graph-1" "http://publisher" "http://freddy"]]
                   [["http://test.com/graph-2" "http://publisher" "http://freddy"]]}
                 (run-query q :->csv? true)))))

      (testing "FROM NAMED allows selection of named triples from only the specified graph(s)"
        (let [q "SELECT ?g ?s
                 FROM NAMED <http://test.com/fake-default-graph-1>
                 WHERE { GRAPH ?g { ?s ?p ?o } }"]
          (is (= #{[["http://test.com/fake-default-graph-1" "http://test.com/graph-1"]]}
                 (run-query q :->csv? true))))

        (let [q "SELECT ?g ?s
                 FROM NAMED <http://test.com/fake-default-graph-1>
                 FROM NAMED <http://test.com/fake-default-graph-2>
                 WHERE { GRAPH ?g { ?s ?p ?o } }"]
          (is (= #{[["http://test.com/fake-default-graph-1" "http://test.com/graph-1"]]
                   [["http://test.com/fake-default-graph-2" "http://test.com/graph-2"]]}
                 (run-query q :->csv? true)))))

      (testing "FROM and FROM NAMED together"
        (let [q "SELECT ?g ?s
                 FROM <http://test.com/fake-default-graph-1>
                 FROM NAMED <http://test.com/graph-1>
                 WHERE { ?g ?x ?y . GRAPH ?g { ?s ?p ?o } }"]
          (is (= #{[["http://test.com/graph-1" "http://test.com/subject-1"]]}
                 (run-query q :->csv? true))))))

    (testing "FROM and FROM NAMED in user protocol request"
      (testing "FROM doesn't allow selection of named triples"
       (let [q "SELECT ?g ?s
                WHERE { GRAPH ?g { ?s ?p ?o } }"]
         (is (= #{}
                (run-query q :default-graph-uri ["http://test.com/fake-default-graph-1"])))))

      (testing "FROM NAMED doesn't allow selection from default graph"
        (let [q "SELECT ?g ?s
                 WHERE { ?s ?p ?o }"]
          (is (= #{}
                 (run-query q :named-graph-uri ["http://test.com/fake-default-graph-1"])))))

      (testing "FROM allows selection of triples from only the specified graph(s)"
        (let [q "SELECT ?s ?p ?o
                 WHERE { ?s ?p ?o }"]
          (is (= #{[["http://test.com/graph-1" "http://publisher" "http://freddy"]]}
                 (run-query q
                            :->csv? true
                            :default-graph-uri ["http://test.com/fake-default-graph-1"]))))

        (let [q "SELECT ?s ?p ?o
                 WHERE { ?s ?p ?o }"]
          (is (= #{[["http://test.com/graph-1" "http://publisher" "http://freddy"]]
                   [["http://test.com/graph-2" "http://publisher" "http://freddy"]]}
                 (run-query q
                            :->csv? true
                            :default-graph-uri ["http://test.com/fake-default-graph-1"
                                                "http://test.com/fake-default-graph-2"])))))

      (testing "FROM NAMED allows selection of named triples from only the specified graph(s)"
        (let [q "SELECT ?g ?s
                 WHERE { GRAPH ?g { ?s ?p ?o } }"]
          (is (= #{[["http://test.com/fake-default-graph-1" "http://test.com/graph-1"]]}
                 (run-query q
                            :->csv? true
                            :named-graph-uri ["http://test.com/fake-default-graph-1"]))))

        (let [q "SELECT ?g ?s
                 WHERE { GRAPH ?g { ?s ?p ?o } }"]
          (is (= #{[["http://test.com/fake-default-graph-1" "http://test.com/graph-1"]]
                   [["http://test.com/fake-default-graph-2" "http://test.com/graph-2"]]}
                 (run-query q
                            :->csv? true
                            :named-graph-uri ["http://test.com/fake-default-graph-1"
                                              "http://test.com/fake-default-graph-2"])))))

      (testing "FROM and FROM NAMED together"
        (let [q "SELECT ?g ?s
                 WHERE { ?g ?x ?y . GRAPH ?g { ?s ?p ?o } }"]
          (is (= #{[["http://test.com/graph-1" "http://test.com/subject-1"]]}
                 (run-query q
                            :->csv? true
                            :default-graph-uri ["http://test.com/fake-default-graph-1"]
                            :named-graph-uri ["http://test.com/graph-1"]))))))))

(deftest-system draft-from-and-from-named-interaction-test
  [{endpoint :drafter.routes.draftsets-api/draftset-query-handler
    api :drafter.routes/draftsets-api
    :keys [drafter.stasher/repo] :as system}
   system-config]
  (let [req (create-test/create-draftset-request test-editor nil nil)
        draftset (-> req api :headers (get "Location"))
        quads (statements "test/resources/drafter/routes/sparql-test/graph-restriction-additions.trig")]
    (append-quads-to-draftset-through-api api test-editor draftset quads)
    (letfn [(run-query [q & kwargs]
              (let [{:keys [->csv?]} (apply hash-map kwargs)
                    request (apply draftset-query draftset q kwargs)
                    process (if ->csv? (partial map csv/parse-csv) identity)]
                (-> request api :body io/reader line-seq rest process set)))]

      (testing "FROM and FROM NAMED in user query\n"

        (testing "FROM doesn't allow selection of named triples"
          (let [q "SELECT ?g ?s
                   FROM <http://test.com/fake-default-graph-3>
                   WHERE { GRAPH ?g { ?s ?p ?o } }"]
            (is (= #{}
                   (run-query q)))))

        (testing "FROM NAMED doesn't allow selection from default graph"
          (let [q "SELECT ?g ?s
                   FROM NAMED <http://test.com/fake-default-graph-3>
                   WHERE { ?s ?p ?o }"]
            (is (= #{}
                   (run-query q)))))

        (testing "FROM allows selection of triples from only the specified graph(s)"
          (let [q "SELECT ?s ?p ?o
                   FROM <http://test.com/fake-default-graph-3>
                   WHERE { ?s ?p ?o }"]
            (is (= #{[["http://test.com/graph-6" "http://publisher" "http://freddy"]]}
                   (run-query q :->csv? true))))

          (let [q "SELECT ?s ?p ?o
                   FROM <http://test.com/fake-default-graph-1>
                   FROM <http://test.com/fake-default-graph-3>
                   WHERE { ?s ?p ?o }"]
            (is (= #{[["http://test.com/graph-1" "http://publisher" "http://freddy"]]
                     [["http://test.com/graph-6" "http://publisher" "http://freddy"]]}
                   (run-query q :->csv? true :union-with-live true)))))

        (testing "FROM NAMED allows selection of named triples from only the specified graph(s)"
          (let [q "SELECT ?g ?s
                   FROM NAMED <http://test.com/fake-default-graph-3>
                   WHERE { GRAPH ?g { ?s ?p ?o } }"]
            (is (= #{[["http://test.com/fake-default-graph-3" "http://test.com/graph-6"]]}
                   (run-query q :->csv? true))))

          (let [q "SELECT ?g ?s
                   FROM NAMED <http://test.com/fake-default-graph-1>
                   FROM NAMED <http://test.com/fake-default-graph-3>
                   WHERE { GRAPH ?g { ?s ?p ?o } }"]
            (is (= #{[["http://test.com/fake-default-graph-1" "http://test.com/graph-1"]]
                     [["http://test.com/fake-default-graph-3" "http://test.com/graph-6"]]}
                   (run-query q :->csv? true :union-with-live true)))))

        (comment

          ;; Currently, this can't work on draftsets because the two `?g`s in
          ;; the WHERE (and GRAPH) clause(s) will not match when `GRAPH ?g` is
          ;; a draft.

          (testing "FROM and FROM NAMED together"
            (let [q "SELECT ?g ?s
                   FROM <http://test.com/fake-default-graph-3>
                   FROM NAMED <http://test.com/graph-6>
                   WHERE { ?g ?x ?y . GRAPH ?g { ?s ?p ?o } }"]
              (is (= #{[["http://test.com/graph-6" "http://test.com/subject-6"]]}
                     (run-query q :->csv? true)))))))

      (testing "FROM and FROM NAMED in user protocol request\n"

        (testing "FROM doesn't allow selection of named triples"
          (let [q "SELECT ?g ?s
                   WHERE { GRAPH ?g { ?s ?p ?o } }"]
            (is (= #{}
                   (run-query q :default-graph-uri ["http://test.com/fake-default-graph-3"])))))

        (testing "FROM NAMED doesn't allow selection from default graph"
          (let [q "SELECT ?g ?s
                   WHERE { ?s ?p ?o }"]
            (is (= #{}
                   (run-query q :named-graph-uri ["http://test.com/fake-default-graph-3"])))))

        (testing "FROM allows selection of triples from only the specified graph(s)"
          (let [q "SELECT ?s ?p ?o
                   WHERE { ?s ?p ?o }"]
            (is (= #{[["http://test.com/graph-6" "http://publisher" "http://freddy"]]}
                   (run-query q
                              :->csv? true
                              :default-graph-uri ["http://test.com/fake-default-graph-3"]))))

          (let [q "SELECT ?s ?p ?o
                   WHERE { ?s ?p ?o }"]
            (is (= #{[["http://test.com/graph-1" "http://publisher" "http://freddy"]]
                     [["http://test.com/graph-6" "http://publisher" "http://freddy"]]}
                   (run-query q
                              :->csv? true
                              :union-with-live true
                              :default-graph-uri ["http://test.com/fake-default-graph-1"
                                                  "http://test.com/fake-default-graph-3"])))))

        (testing "FROM NAMED allows selection of named triples from only the specified graph(s)"
          (let [q "SELECT ?g ?s
                   FROM NAMED <http://test.com/fake-default-graph-3>
                   WHERE { GRAPH ?g { ?s ?p ?o } }"]
            (is (= #{[["http://test.com/fake-default-graph-3" "http://test.com/graph-6"]]}
                   (run-query q
                              :->csv? true
                              :named-graph-uri ["http://test.com/fake-default-graph-3"]))))

          (let [q "SELECT ?g ?s WHERE { GRAPH ?g { ?s ?p ?o } }"]
            (is (= #{[["http://test.com/fake-default-graph-1" "http://test.com/graph-1"]]
                     [["http://test.com/fake-default-graph-3" "http://test.com/graph-6"]]}
                   (run-query q
                              :->csv? true
                              :union-with-live true
                              :named-graph-uri ["http://test.com/fake-default-graph-1"
                                                "http://test.com/fake-default-graph-3"])))))

        (comment

          ;; Currently, this can't work on draftsets because the two `?g`s in
          ;; the WHERE (and GRAPH) clause(s) will not match when `GRAPH ?g` is
          ;; a draft.

          (testing "FROM and FROM NAMED together"
           (let [q "SELECT ?g ?s WHERE { ?g ?x ?y . GRAPH ?g { ?s ?p ?o } }"]
             (is (= #{[["http://test.com/graph-6" "http://test.com/subject-6"]]}
                    (run-query q
                               :->csv? true
                               :default-graph-uri ["http://test.com/fake-default-graph-3"]
                               :named-graph-uri ["http://test.com/graph-6"])))))

          )

        ))))
