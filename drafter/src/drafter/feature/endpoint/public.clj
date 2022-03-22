(ns drafter.feature.endpoint.public
  (:require [integrant.core :as ig]
            [clojure.java.io :as io]
            [grafter-2.rdf.protocols :as gproto]
            [grafter-2.rdf4j.repository :as repo]
            [grafter-2.rdf4j.sparql :as sp]))

(defmacro with-retry [retries sleep & body]
  `(let [start# (System/currentTimeMillis)]
     (loop [retries# ~retries]
       (let [res# (try {:ok (do ~@body)} (catch Exception ex# {:ex ex#}))]
         (if (contains? res# :ok)
           (:ok res#)
           (if (pos? retries#)
             (do
               (Thread/sleep ~sleep)
               (recur (dec retries#)))
             (throw (:ex res#))))))))

(defn ensure-public-endpoint
  "Ensures the public endpoint exists within the endpoints graph, creating it if
   necessary."
  [repo]
  (with-open [conn (repo/->connection repo)]
    (let [u (slurp (io/resource "drafter/feature/endpoint/ensure_public_endpoint.sparql"))]
      (with-retry 60 1000
        (gproto/update! conn u)))))

(defn get-public-endpoint
  "Returns a map representation of the public endpoint"
  [repo]
  (with-open [conn (repo/->connection repo)]
    (let [bindings (vec (sp/query "drafter/feature/endpoint/get_public_endpoint.sparql" conn))]
      (case (count bindings)
        0 nil
        1 (let [{:keys [created modified version]} (first bindings)]
            {:id "public"
             :type "Endpoint"
             :created-at created
             :updated-at modified
             :version version})
        (throw (ex-info "Found multiple public endpoints - expected at most one" {:bindings bindings}))))))

(defmethod ig/init-key ::init [_ {:keys [repo]}]
  (ensure-public-endpoint repo))
