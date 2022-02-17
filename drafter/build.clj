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
               :image-name "swirrl/drafter-pmd4" ; TODO include the destination registry details?
               :image-type (or (opts :image-type) :docker)              
               :include {"/app/config" ["./resources/drafter-auth0.edn"]}
               :base-image "gcr.io/distroless/java:11"
               :volumes #{"/app/config" "/app/stasher-cache"}
               ;; TODO: maybe add registry creds, and tags.
               ; :to-registry {}  ; creds for pushing
               ; :tags #{"latest" "2.5_circle-245"} ; tags
               )
        (pack/docker))))

(comment (pmd4-docker-build nil))

(defn build [opts]
  (-> opts
      (pmd4-docker-build)))

(comment (build nil))
