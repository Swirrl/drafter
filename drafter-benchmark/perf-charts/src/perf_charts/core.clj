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
    (if-let [[_ short-name k-statements graphs ref-statements] (re-find #"^(\w+)Test_(\d+)k_(\d+)g_(\d+)pc?$" benchmark-name)]
      (let [n-statements (* 1000 (Long/parseLong k-statements))
            n-ref (Long/parseLong ref-statements)]
        {:benchmark benchmark-name
         :short-name short-name
         :statements n-statements
         :ref-statements n-ref
         :graphs (Long/parseLong graphs)})
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

(def dim-keys #{:statements :graphs :ref-statements})

(defn- get-benchmark-slices
  "Fixes the given dimension to use as the x-axis and returns a map
  {graph-key result}. Each graph-key contains the x-axis dimension
  along with the values of the other dimensions for each benchmark
  result. Each result map contains a collection of (x, y) co-ordinates
  for the points on the graph. Only experiments containing more than
  one point on the corresponding graph are returned."
  [x-key {:keys [version benchmarks] :as benchmark-set}]
  (let [other-dims (conj (disj dim-keys x-key) :short-name)
        groups (group-by (fn [benchmark]
                           {:x-key x-key :dims (select-keys benchmark other-dims)})
                         benchmarks)]
    (into {} (keep (fn [[k results]]
                     (when (> (count results) 1)
                       [k {:version version
                           :x (map x-key results)
                           :y (map :score results)}]))
                   groups))))

(defn- benchmark-set-slices [benchmark-set]
  (merge (get-benchmark-slices :statements benchmark-set)
         (get-benchmark-slices :graphs benchmark-set)
         (get-benchmark-slices :ref-statements benchmark-set)))

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

(defn collect-graphs [benchmark-sets]
  (let [set-slices (mapv benchmark-set-slices benchmark-sets)
        combined-slices (collect-values set-slices)]
    ;; remove entries that only contain result for one benchmark set
    (into {} (keep (fn [[k results]]
                     (when (> (count results) 1)
                       [k results]))
                   combined-slices))))

(defn- sort-dimensions [{:keys [dims] :as chart-keys}]
  (let [dim-order {:short-name 1 :statements 2 :graphs 3 :ref-statements 4}]
    (sort-by (comp dim-order key) dims)))

(defn- dim-value-formatter
  "Returns a function which formats a dimension-value pair according to
  the dimension type. dim->format-fn should be a map of {dim fmt}
  where fmt is a function from the dimension value to a string."
  [dim->format-fn]
  (fn [[dim value]]
    (if-let [fmt-fn (get dim->format-fn dim)]
      (fmt-fn value))))

(defn- chart-title [{:keys [dims] :as chart-keys}]
  (let [[[_ benchmark-name] & dim-values] (sort-dimensions chart-keys)
        fmt (dim-value-formatter
             {:statements (fn [statements] (format "%d statements" statements))
              :graphs (fn [graphs] (format "%d graphs" graphs))
              :ref-statements (fn [refs] (format "%d%% graph-referencing statements" refs))})
        dim-descs (map fmt dim-values)]
    (format "%s benchmark for %s" benchmark-name (string/join ", " dim-descs))))

(defn- get-x-label [x-key]
  (get {:statements "Number of statements"
        :graphs "Number of graphs"
        :ref-statements "% of graph-referencing statements"}
       x-key))

(defn- chart-file-name [{:keys [dims] :as chart-key}]
  (let [fmt (dim-value-formatter {:statements (fn [statements] (format "%dk" (/ statements 1000)))
                                  :short-name identity
                                  :graphs (fn [graphs] (format "%dg" graphs))
                                  :ref-statements (fn [refs] (format "%dpc" refs))})
        ordered-dim-values (sort-dimensions chart-key)]
    (str (string/join "-" (map fmt ordered-dim-values)) ".png")))

(defn- write-chart [output-dir {:keys [x-key dims] :as chart-key} results]
  (let [plot (chart-results results
                            {:title (chart-title chart-key)
                             :y-label "Seconds"
                             :x-label (get-x-label x-key)})
        file-name (chart-file-name chart-key)]
    (ic/save plot (io/file output-dir file-name))))

(defn read-benchmark-file
  "Reads a set of benchmark results from a benchmark file. The filename
  is expected to have the format jmh-result-{version}.csv where
  version identifies the drafter version used within the benchmarks."
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
  (let [benchmark-sets (mapv read-benchmark-file benchmark-files)
        graphs (collect-graphs benchmark-sets)]
    (doseq [[chart-key results] graphs]
      (write-chart output-dir chart-key results))))
