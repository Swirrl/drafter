(ns drafter.fixture-data
  (:require [clojure.test :as t]
            [grafter-2.rdf4j.repository :as repo]
            [grafter-2.rdf4j.io :as rio]
            [grafter-2.rdf.protocols :as pr]
            [clojure.java.io :as io]
            [integrant.core :as ig]
            [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log])
  (:import org.eclipse.rdf4j.repository.Repository))

(defn drop-all! [repo]
  (log/debug "Tearing down fixtures")
  (with-open [conn (repo/->connection repo)]
    (pr/update! conn "DROP ALL ;")))

(defn load-fixture! [{:keys [repo fixtures format] :as opts}]
  (with-open [conn (repo/->connection repo)]
    (doseq [res-path fixtures]
      (pr/add conn (rio/statements res-path :format format))))
  (log/debug "Loaded fixtures" fixtures)
  (assoc opts :repo repo))

(s/def ::resource #(instance? java.net.URL %)) ;; io/resource creates urls.

(s/def ::repo #(instance? Repository %))

(s/def ::fixtures (s/coll-of ::resource))

(defmethod ig/pre-init-spec ::loader [_]
  (s/keys :req-un [::repo ::fixtures]))

(defmethod ig/init-key ::loader [_ opts]
  (drop-all! (:repo opts))
  (load-fixture! opts))
