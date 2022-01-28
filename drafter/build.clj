(ns build
  (:require [clojure.tools.build.api :as b]
            [clojure.string :as str]
            [juxt.pack.api :as pack])
  (:refer-clojure :exclude [test]))

(defn- get-branch-name []
  (or (System/getenv "CIRCLE_BRANCH")
      (b/git-process {:git-args ["branch" "--show-current"]})))

(defn- cleanup-branch-name [branch-str]
  (str/replace branch-str #"\\" "_"))

(defn main-build? []
  (and (System/getenv "CIRCLE_BRANCH")
       (= "master" (get-branch-name))))

(def version-prefix "1.1")

(defn version []
  (let [current-branch (cleanup-branch-name
                        (get-branch-name))]
    (str version-prefix "."
         (if (main-build?)
           (b/git-count-revs nil)
           (str (b/git-count-revs nil) "-BRANCH-BUILD-" current-branch)))))

(comment
  (version)
  )

(defn pmd4-docker-build [opts]
  (let [drafter-basis (b/create-basis {:project "deps.edn" :aliases [:pmd4/docker]})]
    (-> opts
        (assoc :basis drafter-basis
               :image-name "swirrl/drafter-pmd4"
               :image-type (if (main-build?)
                             :registry ; push to registry if in CI
                             :docker   ; otherwise push to dockerd locally
                             )
               :include {"/app/config" ["./resources/drafter-auth0.edn"]}
               :base-image "gcr.io/distroless/java:11"
               :volumes #{"/app/config" "/app/stasher-cache"})
        (pack/docker))))

(comment (pmd4-docker-build nil))

(defn build [opts]
  (-> opts
      (pmd4-docker-build)))

(comment (build nil))
