(ns drafter.fixture-data
  (:require [clojure.test :as t]
            [grafter.rdf4j.repository :as repo]
            [grafter.rdf :as rdf]
            [grafter.rdf.protocols :as pr]
            [clojure.java.io :as io]
            [integrant.core :as ig]
            [clojure.spec.alpha :as s])
  (:import org.eclipse.rdf4j.repository.Repository))


(defn load-fixture! [{:keys [repo fixtures format] :as opts}]
  (with-open [conn (repo/->connection repo)]
    (doseq [res-path fixtures]
      (rdf/add conn (rdf/statements res-path :format format))))
  (assoc opts :repo repo))

(s/def ::resource #(instance? java.net.URL %)) ;; io/resource creates urls.

(s/def ::repo #(instance? Repository %))

(s/def ::fixtures (s/coll-of ::resource)) 

(defmethod ig/pre-init-spec ::loader [_]
  (s/keys :req-un [::repo ::fixtures]))

(defmethod ig/init-key ::loader [_ opts]
  (load-fixture! opts))

(defmethod ig/halt-key! ::loader [_ {:keys [repo]}]
  (with-open [conn (repo/->connection repo)]
    (pr/update! conn "DROP ALL ;")))
