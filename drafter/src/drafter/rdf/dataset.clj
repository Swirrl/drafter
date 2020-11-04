(ns drafter.rdf.dataset
  "Namespace for representing SPARQL query datasets and converting to and from the
   Jena and RDF4j representations."
  (:require [grafter.url :as url]
            [grafter-2.rdf4j.repository :as repo]
            [clojure.set :as set]
            [grafter-2.rdf4j.io :as gio])
  (:import [org.apache.jena.sparql.core DatasetDescription]
           [java.net URI]
           [org.eclipse.rdf4j.query.impl SimpleDataset]))

(defprotocol IntoDataset
  (->dataset [this]))

(def empty-dataset {:named-graphs #{} :default-graphs #{}})

(defn- empty-dataset? [{:keys [named-graphs default-graphs]}]
  (and (empty? named-graphs) (empty? default-graphs)))

(defn create
  "Creates a dataset containing the specified named and default graphs"
  [& {:keys [named-graphs default-graphs]}]
  {:named-graphs   (set (map url/->java-uri named-graphs))
   :default-graphs (set (map url/->java-uri default-graphs))})

(extend-protocol IntoDataset
  DatasetDescription
  (->dataset [^DatasetDescription dataset]
    (if (nil? dataset)
      empty-dataset
      (letfn [(->uri-set [uris] (set (map #(URI. %) uris)))]
        {:named-graphs   (->uri-set (.getNamedGraphURIs dataset))
         :default-graphs (->uri-set (.getDefaultGraphURIs dataset))})))

  org.eclipse.rdf4j.query.Dataset
  (->dataset [^org.eclipse.rdf4j.query.Dataset dataset]
    {:named-graphs   (set (map url/->java-uri (.getNamedGraphs dataset)))
     :default-graphs (set (map url/->java-uri (.getDefaultGraphs dataset)))})

  org.apache.jena.query.Query
  (->dataset [^org.apache.jena.query.Query query]
    (->dataset (.getDatasetDescription query)))

  nil
  (->dataset [_dataset]
    empty-dataset))

(defn ->rdf4j-dataset
  "Converts a dataset into a RDF4j Dataset"
  [{:keys [named-graphs default-graphs] :as dataset}]
  (let [rdf4j-dataset (SimpleDataset.)]
    (doseq [named-graph named-graphs]
      (.addNamedGraph rdf4j-dataset (gio/->rdf4j-uri named-graph)))
    (doseq [default-graph default-graphs]
      (.addDefaultGraph rdf4j-dataset (gio/->rdf4j-uri default-graph)))
    rdf4j-dataset))

(defn- ->restricted-dataset
  "Returns an RDF4j dataset which can be used as a dataset on restricted repositories.
   If the input dataset is empty the returned dataset will contain a default graph containing
   URIs unlikely to exist in the data."
  [{:keys [named-graphs default-graphs]}]
  (repo/make-restricted-dataset
    :named-graphs (map str named-graphs)
    :default-graph (map str default-graphs)))

(defn- resolve-dataset
  "Resolves which dataset to use"
  [query-dataset user-dataset]
  (if-not (empty-dataset? user-dataset)
    user-dataset
    query-dataset))

(defn- restrict-dataset
  "Restricts the graphs within a dataset to those present in the set of visible graphs"
  [dataset visible-graphs]
  (if (empty-dataset? dataset)
    {:named-graphs   visible-graphs
     :default-graphs visible-graphs}
    (-> dataset
        (update :named-graphs set/intersection visible-graphs)
        (update :default-graphs set/intersection visible-graphs))))

(defn get-query-dataset
  "Returns an RDF4j dataset containing the effective dataset for a query against an endpoint
   restricted to a set of visible graphs. The user dataset is one optionally supplied to the
   query execution and the query dataset is the one declared in the query by FROM and FROM NAMED
   clauses."
  [query-dataset user-dataset visible-graphs]
  (let [dataset (resolve-dataset query-dataset user-dataset)
        dataset (restrict-dataset dataset visible-graphs)]
    (->restricted-dataset dataset)))
