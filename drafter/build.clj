(ns build
  (:require [clojure.tools.build.api :as b]
            [clojure.string :as str]
            [juxt.pack.api :as pack]))

;; A tag name must be valid ASCII and may contain lowercase and uppercase
;; letters, digits, underscores, periods and dashes
;; https://docs.docker.com/engine/reference/commandline/tag/#extended-description
(defn tag [s]
  (str/replace s #"[^a-zA-Z0-9_.-]" "_"))

(def repo "europe-west2-docker.pkg.dev/swirrl-devops-infrastructure-1/swirrl")

(defn pmd4-docker-build [opts]
  (let [tags (map #(tag (b/git-process {:git-args %}))
                  ["rev-parse HEAD"
                   "describe --tags"
                   "branch --show-current"])]
    (pack/docker
     {:basis (b/create-basis {:project "deps.edn" :aliases [:pmd4/docker]})
      ;; If we don't include a tag in the :image-name, then pack implicitly
      ;; tags the image with latest, even when we specify additional tags. So
      ;; choose a tag arbitrarily to be part of the :image-name, and then
      ;; provide the rest in :tags.
      :image-name (str repo "/drafter-pmd4:" (first tags))
      :tags (set (rest tags))
      :image-type (get opts :image-type :docker)
      :include {"/app/config" ["./resources/drafter-auth0.edn"]}
      :base-image "gcr.io/distroless/java:11"
      :volumes #{"/app/config" "/app/stasher-cache"}
      ;; NOTE Not as documented!
      ;; The docstring states that these should be
      ;;     :to-registry {:username ... :password ...}
      ;; but alas, that is a lie.
      ;; https://github.com/juxt/pack.alpha/issues/101
      :to-registry-username "_json_key"
      :to-registry-password (System/getenv "GCLOUD_SERVICE_KEY")})))
