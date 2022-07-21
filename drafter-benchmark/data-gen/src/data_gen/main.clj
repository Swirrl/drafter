(ns data-gen.main
  (:gen-class)
  (:require [clojure.java.io :as io]
            [clojure.tools.cli :as cli]
            [data-gen.core :as core]
            [grafter-2.rdf.protocols :as pr]
            [grafter-2.rdf4j.io :as gio]
            [clojure.test.check.generators :as gen]))

(def cli-options
  [["-g" "--graphs N" "Number of graphs to generate"
    :parse-fn #(Integer/parseInt %)
    :validate [pos? "Must be positive"]]
   ["-s" "--statements N" "Total number of statements to generate"
    :parse-fn #(Integer/parseInt %)
    :validate [pos? "Must be positive"]]
   ["-o" "--output-file FILE" "Output file to write to"
    :parse-fn io/file]])

(defn- generate [n-statements n-graphs out-file]
  (with-open [w (io/writer out-file)]
    (let [rdf-writer (gio/rdf-writer w :format :nq)
          graphs (core/generate-graphs n-graphs)
          graph-sizes (core/sample-1 (core/buckets-gen n-statements n-graphs))]
      (doseq [[g n-triples] (zipmap graphs graph-sizes)]
        (let [qg (core/graph-quad-gen g graphs)
              quads (core/generate-n qg n-triples)]
          (pr/add rdf-writer quads))))))

(defn- verify
  "Reads a generated file and returns the total number of statements
  along with the number of statements in each graph"
  [gen-file]
  (with-open [r (io/reader gen-file)]
    (reduce (fn [acc {:keys [c] :as statement}]
              (-> acc
                  (update :total (fnil inc 0))
                  (update-in [:graphs c] (fnil inc 0))))
            {}
            (gio/statements r :format :nq))))

(defn -main [& args]
  (let [{:keys [options errors summary]} (cli/parse-opts args cli-options)]
    (when (seq errors)
      (binding [*out* *err*]
        (println "Invalid arguments:")
        (doseq [err errors]
          (println err))

        (println "Usage:")
        (println summary))
      (System/exit 1))

    (let [{:keys [graphs statements output-file]} options]
      (println options)
      (generate statements graphs output-file))))
