(ns build
  (:require [clojure.tools.build.api :as b]
            [clojure.string :as str]
            [juxt.pack.api :as pack])
  (:refer-clojure :exclude [test]))

;; A tag name must be valid ASCII and may contain lowercase and uppercase
;; letters, digits, underscores, periods and dashes
;; https://docs.docker.com/engine/reference/commandline/tag/#extended-description
(defn tag [s]
  (str/replace s #"[^a-zA-Z0-9_.-]" "_"))

(defn pmd4-docker-build [opts]
  (let [drafter-basis (b/create-basis {:project "deps.edn" :aliases [:pmd4/docker]})]
    (-> opts
        (assoc :basis drafter-basis
               :image-name "swirrl/drafter-pmd4" ; TODO include the destination registry details?
               :image-type (or (opts :image-type) :docker)
               :include {"/app/config" ["./resources/drafter-auth0.edn"]}
               :base-image "gcr.io/distroless/java:11"
               :volumes #{"/app/config" "/app/stasher-cache"}
               :to-registry-username (System/getenv "DOCKERHUB_USERNAME")
               :to-registry-password (System/getenv "DOCKERHUB_PASSWORD")
               :tags (into #{}
                           (map #(tag (b/git-process {:git-args %})))
                           ["describe --tags"
                            "rev-parse HEAD"
                            "branch --show-current"]))
        (pack/docker))))

(comment (pmd4-docker-build nil))

(defn build [opts]
  (-> opts
      (pmd4-docker-build)))

(comment (build nil))
