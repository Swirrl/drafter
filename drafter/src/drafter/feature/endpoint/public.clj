(ns drafter.feature.endpoint.public
  (:require [integrant.core :as ig]
            [clojure.java.io :as io]
            [grafter-2.rdf.protocols :as gproto]
            [grafter-2.rdf4j.repository :as repo]
            [grafter-2.rdf4j.sparql :as sp]
            [drafter.endpoint :as ep]
            [clojure.spec.alpha :as s]))

(defn ensure-public-endpoint
  "Ensures the public endpoint exists within the endpoints graph, creating it if
   necessary."
  [repo]
  (with-open [conn (repo/->connection repo)]
    (let [u (slurp (io/resource "drafter/feature/endpoint/ensure_public_endpoint.sparql"))]
      (gproto/update! conn u))))

(defn get-public-endpoint
  "Returns a map representation of the public endpoint"
  [repo]
  (with-open [conn (repo/->connection repo)]
    (let [bindings (vec (sp/query "drafter/feature/endpoint/get_public_endpoint.sparql" conn))]
      (case (count bindings)
        0 nil
        1 (let [{:keys [created modified]} (first bindings)]
            {:id "public" :type "Endpoint" :created-at created :updated-at modified})
        (throw (ex-info "Found multiple public endpoints - expected at most one" {:bindings bindings}))))))

(s/fdef get-public-endpoint
  :args (s/cat :repo any?)
  :ret (s/nilable ::ep/Endpoint))
