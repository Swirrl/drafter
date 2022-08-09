(ns perf-charts.core
  (:require [incanter.core :as ic]
            [incanter.charts :as charts]
            [incanter.stats :as stats]
            [incanter.io :as iio]
            [clojure.core.matrix.dataset :as ds]
            [clojure.java.io :as io]
            [clojure.string :as string]))

(defn chart-results
  "Returns a chart for the given collection of series."
  [results opts]
  {:pre (seq results)}
  (let [[{:keys [x y version] :as result} & results] results
        p (charts/xy-plot x y
                          :series-label version
                          :title (:title opts)
                          :x-label (:x-label opts)
                          :y-label (:y-label opts)
                          :legend true)]
    (reduce (fn [p {:keys [x y version] :as result}]
              (charts/add-lines p x y :series-label version))
            p
            results)))

(defn parse-benchmark-name
  "Parse a fully-qualified benchmark name to extract the short name of
  the benchmark, the total number of statements and the number of
  graphs."
  [fq-benchmark-name]
  (let [benchmark-name (last (string/split fq-benchmark-name #"\."))]
    (if-let [[_ short-name k-statements graphs] (re-find #"(\w+)Test_(\d+)k_(\d+)g" benchmark-name)]
      {:benchmark benchmark-name
       :short-name short-name
       :statements (* 1000 (Long/parseLong k-statements))
       :graphs (Long/parseLong graphs)}
      (throw (ex-info (format "Failed to parse benchmark name %s" fq-benchmark-name) {})))))

(defn read-benchmarks
  "Reads a collection of benchmark results from a benchmark file"
  [f]
  (let [ds (iio/read-dataset f :header true)
        records (ds/row-maps ds)]
    (map (fn [rec]
           (assoc (parse-benchmark-name (:Benchmark rec))
                  :score (:Score rec)
                  :unit (:Unit rec)))
         records)))

(defn- map-values [f m]
  (into {} (map (fn [[k v]] [k (f v)]) m)))

(defn- group-benchmarks [group-key x-key {:keys [version benchmarks]}]
  (let [groups (group-by (juxt :short-name group-key) benchmarks)]
    (map-values (fn [results]
                         {:version version
                          :x (map x-key results)
                          :y (map :score results)})
                       groups)))

(defn collect-values
  "Collects the keys across a collection of maps into a single map
  containing a vector of values e.g.

    (collect-values [{:a 1 :b 2} {:a 3 :b 4 :c 5}])
    => {:a [1 3] :b [2 4] :c [5]}
  "
  [ms]
  (->> ms
       (map (fn [m] (map-values (fn [x] [x]) m)))
       (apply merge-with concat)
       (map-values vec)))

(defn series-group [group-key x-key benchmarks]
  (let [gs (map (fn [b] (group-benchmarks group-key x-key b)) benchmarks)]
    (collect-values gs)))

(defn by-statements-series
  "Given a collection of benchmarks, returns a map of the form
  {[benchmark-name statements] [series]} where each series is take
  from the corresponding results in each benchmark results file."
  [benchmarks]
  (series-group :statements :graphs benchmarks))

(defn by-graph-series
  "Given a collection of benchmarks, returns a map of the form
  {[benchmark-name graphs] [series]} where each series is taken from
  the corresponding results in each benchmark results file."
  [benchmarks]
  (series-group :graphs :statements benchmarks))

(defn- generate-by-graph
  "Groups all benchmark results by the number of graphs in the input
  data across all benchmark result files. Renders a chart for
  each (benchmark, num graphs) in the results and writes the chart as
  a .png file to the output directory."
  [output-dir benchmarks]
  (let [by-graph (by-graph-series benchmarks)]
    (doseq [[[short-name graphs] results] by-graph]
      (let [plot (chart-results results {:title (format "%s benchmark for %d graphs" short-name graphs)
                                         :y-label "Seconds"
                                         :x-label "Number of statements"})
            file-name (format "%s-%d-graphs.png" short-name graphs)]
        (ic/save plot (io/file output-dir file-name))))))

(defn- generate-by-statements
  "Groups all benchmark results by the number of statements in the input
  data across all benchmark result files. Renders a chart for
  each (benchmark, num statements) in the results and writes the chart
  as a .png file to the output directory."
  [output-dir benchmarks]
  (let [by-statements (by-statements-series benchmarks)]
    (doseq [[[short-name statements] results] by-statements]
      (let [plot (chart-results results
                                {:title (format "%s benchmark for %d statements" short-name statements)
                                 :y-label "Seconds"
                                 :x-label "Number of graphs"})
            file-name (format "%s-%dk.png" short-name (/ statements 1000))]
        (ic/save plot (io/file output-dir file-name))))))

(defn benchmark-file
  "Reads benchmark results from a benchmark file. The filename is
  expected to have the format jmh-result-{version}.csv where version
  identifies the drafter version used within the benchmarks."
  [file]
  (if-let [[_ version] (re-find #"^jmh-result-(\w+).csv$" (.getName file))]
    {:file file :version version :benchmarks (read-benchmarks file)}
    (throw (ex-info "Unexpected result file name format - expected 'jmh-result-{version}.csv'" {:file file}))))

(defn generate-charts
  "Generates charts for a collection of benchmark result files and
  writes them to the specified output directory. Each benchmark file
  should contain a collection of benchmark results for a specific
  drafter version. Benchmarks are grouped by the number of statements
  and the number of graphs to show how performance is affected as one
  increases and the other is kept constant. The results for each group
  are plotted on each output graph to show the comparison between the
  drafter versions used to generate each benchmark result file."
  [output-dir benchmark-files]
  (let [benchmarks (mapv benchmark-file benchmark-files)]
    (generate-by-graph output-dir benchmarks)
    (generate-by-statements output-dir benchmarks)))
