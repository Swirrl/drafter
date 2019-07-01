(ns package
  (:require [bultitude.core :as b]
            [badigeon.bundle :refer [bundle make-out-path]]
            [badigeon.classpath :as classpath]
            [badigeon.compile :as c]))

(defn -main []
  (let [cp (classpath/make-classpath {:aliases [:prod]})]
    (doseq [ns (sort (b/namespaces-on-classpath :prefix "drafter" :classpath cp))]
      (println "Compiling:" ns)
      (c/compile ns {:compile-path "target/classes" :classpath cp}))))
