(ns drafter-client.test-helpers
  (:require [clojure.spec.test.alpha :as st]
            [clojure.test :as t :refer :all]
            [drafter.main :as main]
            [drafter.middleware.auth0-auth]
            [drafter.middleware.auth]
            [drafter-client.client :as client]
            [drafter-client.test-util.db :as db-util]
            [environ.core :refer [env]]
            [grafter-2.rdf4j.repository :as gr-repo]
            [grafter-2.rdf4j.sparql :as sp]
            [clojure.java.io :as io]
            [grafter-2.rdf.protocols :as pr])
  (:import [java.net URI]))

(defn with-spec-instrumentation [f]
  (try
    (st/instrument)
    (f)
    (catch Throwable e
      (prn e)
      (throw e))
    (finally
      (st/unstrument))))

(defn- get-stardog-repo []
  (let [stardog-query (env :sparql-query-endpoint)
        stardog-update (env :sparql-update-endpoint)]
    (assert stardog-query "Set SPARQL_QUERY_ENDPOINT to run these tests.")
    (assert stardog-update "Set SPARQL_UPDATE_ENDPOINT to run these tests.")
    (gr-repo/sparql-repo stardog-query stardog-update)))

(defn db-fixture [f]
  (let [stardog-repo (get-stardog-repo)]
    (when-not (Boolean/parseBoolean (env :disable-drafter-cleaning-protection))
      (db-util/assert-empty stardog-repo))
    (f)
    (db-util/delete-test-data! stardog-repo)))

(defn drop-test-db!
  "Drop all test data from the backend stardog database"
  []
  (let [stardog-repo (get-stardog-repo)]
    (db-util/drop-all! stardog-repo)))

(defn res-file
  "Convert a resource into a file path, useful to simplify finding
  config files for drafter-server."
  [filename]
  (or (some-> filename io/resource io/file .getCanonicalPath)
      (throw (Exception. (format "Cannot find %s on resource path" filename)))))

(defn start-auth0-drafter-server []
  (main/-main (res-file "auth0-test-config.edn")
              (res-file "stasher-off.edn")
              (res-file "init-public-endpoint.edn")))

(defn start-basicauth-drafter-server []
  (main/-main (res-file "basicauth-test-config.edn")
              (res-file "stasher-off.edn")
              (res-file "init-public-endpoint.edn")))

(defn stop-drafter-server []
  (main/stop-system!))

(def test-triples-filename
  "Some test data as a file of n-triples"
  "resources/specific_mappingbased_properties_bg.nt")

(def modified-times-graph-uri (URI. "http://publishmydata.com/graphs/drafter/graph-modified-times"))

(defn get-user-quads [client token draftset]
  (letfn [(is-user-quad? [quad]
            (not= modified-times-graph-uri (pr/context quad)))]
    (let [all-quads (client/get client token draftset)]
      (filter is-user-quad? all-quads))))

(defn query-user-triples [conn]
  (sp/query "construct_user_triples.sparql" {:systemgraph [modified-times-graph-uri]} conn))
