(ns perf-charts.main
  (:require [perf-charts.core :as core]
            [clojure.java.io :as io]
            [clojure.tools.cli :as cli]))

(def cli-options
  [["-d" "--directory DIR" "Output directory"
    :parse-fn io/file]])

(defn -main [& args]
  (let [{:keys [options arguments]} (cli/parse-opts args cli-options)
        benchmark-files (map io/file arguments)
        output-dir (:directory options)]
    (if-let [output-dir (:directory options)]
      (do
        (.mkdirs output-dir)
        (core/generate-charts output-dir benchmark-files))
      (binding [*out* *err*]
        (println "Usage: perf-charts -d DIRECTORY file...")
        (System/exit 1)))))
