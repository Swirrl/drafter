(ns data-gen.main
  (:gen-class)
  (:require [clojure.java.io :as io]
            [clojure.tools.cli :as cli]
            [clojure.java.shell :as sh]
            [data-gen.core :as core]
            [grafter-2.rdf.protocols :as pr]
            [grafter-2.rdf4j.io :as gio]
            [clojure.test.check.generators :as gen]))

(defn- parse-referential [s]
  (if-let [[_ ns] (re-find #"^(\d+)%?$" s)]
    (let [n (Long/parseLong ns)]
      (if (.endsWith s "%")
        (if (and (>= n 0) (<= n 100))
          [:percentage n]
          "Invalid percentage")
        [:absolute n]))
    "Invalid specifier"))

(defmulti get-graph-referencing-statement-count (fn [ref-spec _n-statements]
                                                  (first ref-spec)))

(defmethod get-graph-referencing-statement-count :absolute [[_ n] n-statements]
  (min n n-statements))

(defmethod get-graph-referencing-statement-count :percentage [[_ pc] n-statements]
  (cond
    (= 0 pc) 0
    (= 100 pc) n-statements
    :else (long (* n-statements (/ pc 100)))))

(def cli-options
  [["-g" "--graphs N" "Number of graphs to generate"
    :parse-fn #(Integer/parseInt %)
    :validate [pos? "Must be positive"]]
   ["-s" "--statements N" "Total number of statements to generate"
    :parse-fn #(Integer/parseInt %)
    :validate [pos? "Must be positive"]]
   ["-o" "--output-file FILE" "Output file to write to"
    :parse-fn io/file]
   ["-r" "--referential REF" "Either the total number 'n' or the percentage 'n%' of graph-referencing statements to generate"
    :parse-fn parse-referential
    :validate [vector? "Invalid referential specifier - should be number 'n' or 'n%' of generated statements"]
    :default [:absolute 0]]])

(defn- dist-ref-statements
  "Distributes n graph-referencing statements between a collection of
  graphs with the given sizes. The distribution is done uniformly
  across all non-empty graphs in the collection. Note the method of
  distribution is biased towards graphs at the beginning of the
  collection. Returns a vectory with the same length as the input
  which indicates the number of graph-referencing statements to
  allocate to each input graph."
  [n-ref graph-sizes]
  (let [n-statements (reduce + 0 graph-sizes)
        avg-per-graph (/ n-ref n-statements)]
    (loop [remaining n-ref
           graph-sizes graph-sizes
           acc []]
      (cond
        (zero? remaining)
        (vec (concat acc (repeat (count graph-sizes) 0)))

        (seq graph-sizes)
        (let [graph-size (first graph-sizes)
              expected-nref (* graph-size avg-per-graph)
              graph-ref (min remaining (long (Math/ceil expected-nref)))]
          (recur (- remaining graph-ref) (rest graph-sizes) (conj acc graph-ref)))

        :else
        acc))))

(defn- generate [n-statements n-ref n-graphs out-file]
  (with-open [w (io/writer out-file)]
    (let [rdf-writer (gio/rdf-writer w :format :nq)
          graphs (core/generate-graphs n-graphs)
          graph-sizes (core/sample-1 (core/buckets-gen n-statements n-graphs))
          graph-nrefs (dist-ref-statements n-ref graph-sizes)]
      (doseq [[g n-triples ref-triples-count] (map vector graphs graph-sizes graph-nrefs)]
        (let [non-ref-triples-count (- n-triples ref-triples-count)
              quads (concat (core/generate-n (core/graph-ref-quad-gen g graphs) ref-triples-count)
                            (core/generate-n (core/non-ref-quad-gen g) non-ref-triples-count))]
          (pr/add rdf-writer quads))))))

(defn- verify-task
  "Reads a generated file and returns the total number of statements
  along with the number of statements in each graph"
  [{:keys [file]}]
  (with-open [r (io/reader file)]
    (reduce (fn [acc {:keys [c] :as statement}]
              (-> acc
                  (update :total (fnil inc 0))
                  (update-in [:graphs c] (fnil inc 0))))
            {}
            (gio/statements r :format :nq))))

(defn generate-task [{:keys [graphs statements referential output-file]}]
  (let [n-ref (get-graph-referencing-statement-count referential statements)]
    (generate statements n-ref graphs output-file)))

(defn generate-all-task [{:keys [output-dir] :as opts}]
  (.mkdirs output-dir)
  (let [k-statements [1 10 100 1000]
        graphs [1 10 100 200]
        ref-percentages [0 1 5 10]]
    (doseq [[ks g r] (for [ks k-statements
                           g graphs
                           r ref-percentages]
                       [ks g r])]
      (let [file-name (format "data_%dk_%dg_%dpc.nq" ks g r)
            data-file (io/file output-dir file-name)
            delete-file (io/file output-dir (str file-name ".delete"))
            n-statements (* 1000 ks)]
        (when-not (.exists data-file)
          (generate-task {:graphs g
                          :statements n-statements
                          :referential [:percentage r]
                          :output-file (io/file output-dir file-name)}))

        (when-not (.exists delete-file)
          (let [{:keys [exit err]} (sh/sh "/bin/bash" "-c" (format "head -n %d %s > %s" (long (/ n-statements 2)) (.getAbsolutePath data-file) (.getAbsolutePath delete-file)))]
            (when-not (= 0 exit)
              (binding [*out* *err*]
                (println "Failed to create deletion file" (.getAbsolutePath delete-file))
                (println (slurp err))))))))))

(def tasks {"verify" {:options [["-f" "--file FILE" "The file to validate"
                                 :parse-fn io/file
                                 :validate [#(.exists %) "File does not exist"]]]
                      :task-fn verify-task}
            "generate" {:options cli-options
                        :task-fn generate-task}
            "generate-all" {:options [["-o" "--output-dir DIR" "Output directory to write files to"
                              :parse-fn io/file]]
                   :task-fn generate-all-task}})

(defn -main [& args]
  (if-let [task-name (first args)]
    (if-let [{:keys [options task-fn] :as task} (get tasks task-name)]
      (let [{:keys [options errors summary]} (cli/parse-opts (rest args) options)]
        (when (seq errors)
          (binding [*out* *err*]
            (println "Invalid arguments:")
            (doseq [err errors]
              (println err))

            (println "Usage:")
            (println summary))
          (System/exit 1))

        (task-fn options))

      (binding [*out* *err*]
        (println (format "Unknown task name '%s'" task-name))
        (System/exit 1)))
    
    (binding [*out* *err*]
      (println "Usage: data-gen task [args]")
      (System/exit 1))))
