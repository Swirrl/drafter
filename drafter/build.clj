(ns build
  (:require [clojure.tools.build.api :as b]
            [clojure.string :as str]
            [juxt.pack.api :as pack]))

;; A tag name must be valid ASCII and may contain lowercase and uppercase
;; letters, digits, underscores, periods and dashes
;; https://docs.docker.com/engine/reference/commandline/tag/#extended-description
(defn tag [s]
  (str/replace s #"[^a-zA-Z0-9_.-]" "_"))

(defn pmd4-docker-build [opts]
  (pack/docker
   {:basis (b/create-basis {:project "deps.edn" :aliases [:pmd4/docker]})
    :image-name "europe-west2-docker.pkg.dev/swirrl-devops-infrastructure-1/swirrl/drafter-pmd4"
    :image-type (get opts :image-type :docker)
    :include {"/app/config" ["./resources/drafter-auth0.edn"]}
    :base-image "gcr.io/distroless/java:11"
    :volumes #{"/app/config" "/app/stasher-cache"}
    :to-registry-username "_json_key"
    :to-registry-password (System/getenv "GCLOUD_SERVICE_KEY")
    :tags (into #{}
                (map #(tag (b/git-process {:git-args %})))
                ["describe --tags"
                 "rev-parse HEAD"
                 "branch --show-current"])}))
