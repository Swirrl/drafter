(ns drafter-client.client-basicauth-test
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.set :as set]
            [clojure.test :as t :refer [deftest is testing]]
            [drafter-client.auth.basic-auth :as ba]
            [drafter-client.test-helpers :as h]
            [drafter.main :as drafter]
            [environ.core :refer [env]]
            [drafter-client.client :as sut]
            [drafter-client.client.repo :as repo]
            [grafter-2.rdf4j.repository :as grepo])
  (:import java.net.URI))

(def basicauth-config (h/res-file "basicauth-test-config.edn"))

(defn start-basicauth-drafter-server []
  (drafter/-main basicauth-config
                 (h/res-file "stasher-off.edn")
                 (h/res-file "init-public-endpoint.edn")))

(defn drafter-server-fixture [f]
  (try
    (h/drop-test-db!)
    (start-basicauth-drafter-server)
    (f)
    (finally
      (h/stop-drafter-server))))

(t/use-fixtures :once
  drafter-server-fixture)

(t/use-fixtures :each
  h/with-spec-instrumentation
  ;; Drop db after tests
  h/db-fixture)

(defn get-user
  "Lookup and return a user from the basicauth-test-config.edn we're
  using to configure drafters in memory user db."
  [username]
  (let [user-db (->> basicauth-config
                     slurp
                     (edn/read-string {:default hash-map})
                     :drafter.user/memory-repository
                     :users
                     (group-by :username))]
    (-> user-db
        (get username)
        first)))

(defn basicauth-drafter-client
  "Return a basic auth drafter-client configured with authorization for
  the specified username.  User is looked up from drafters config."
  [username]
  (let [drafter-endpoint (env :drafter-endpoint)
        basicauth-provider (-> (get-user username)
                               (set/rename-keys {:username :user})
                               (select-keys [:user :password])
                               ba/map->BasicAuthProvider)]
    (assert drafter-endpoint "Set DRAFTER_ENDPOINT to run these tests.")
    (sut/client drafter-endpoint
                :auth-provider basicauth-provider)))

(defn sample-endpoint-triples [repo]
  (with-open [conn (grepo/->connection repo)]
    (into [] (grepo/query conn "construct { ?s ?p ?o } where { ?s ?p ?o } limit 10"))))

(deftest basicauth-draftset-test

  ;; NOTE the drafter-client implementation at the time of writing
  ;; contains essentially 3 different code paths for interaction:
  ;;
  ;; P1. Interactions that use the martian client
  ;; P2. Interactions that stream large files of RDF data (using the clj-http underlying martian)
  ;; P3. Interactions that use grafter/RDF4j's sparql-repo interfaces
  ;;
  ;; Hence rather than retesting every method for the basicauth
  ;; auth-provider we test just enough to ensure we cover these main
  ;; paths.
  ;;
  ;; new-draftset / get-draftsets tests P1.
  ;; add-data-sync with a file of RDF tests P2.
  ;; make-repo / query tests P3.


  (testing "basicauth simple draftset ops"
    (let [client (basicauth-drafter-client "manager@swirrl.com")]
      (testing "new-draftset & get-draftsets"
        (is (sut/new-draftset client nil "new draftset" "my new draftset"))
        (let [{:keys [id name description] :as draftset} (first (sut/get-draftsets client nil))
              graph (URI. "http://test.graph.com/triple-graph")]
          (is (uuid? id))
          (is (= "new draftset" name))
          (is (= "my new draftset" description))
          (testing "adding / querying data"
            (let [repo (repo/make-repo client draftset nil {})]

              (is (empty? (sample-endpoint-triples repo))
                  "Draftset should be empty (as no public triples and draftset is fresh)")

              (sut/add-data-sync client nil draftset (io/file (h/res-file h/test-triples-filename)) {:graph graph})
              (is (pos-int? (count (sample-endpoint-triples repo)))
                  "Draftset contains uploaded triples"))))))))

(comment

  (start-basicauth-drafter-server)

  (def client (basicauth-drafter-client "system@swirrl.com"))

  (h/stop-drafter-server)

  )
