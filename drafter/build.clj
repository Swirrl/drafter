(ns build
  (:require [clojure.tools.build.api :as b]
            [clojure.string :as str]
            [juxt.pack.api :as pack]))

(def lib 'com.swirrl/drafter)
(def version (format "2.0.%s" (b/git-count-revs nil)))
(def class-dir "target/classes")
(def jar-file (format "target/drafter-%s.jar" version))
(def uberjar-file (format "target/drafter-%s-standalone.jar" version))

;; A tag name must be valid ASCII and may contain lowercase and uppercase
;; letters, digits, underscores, periods and dashes
;; https://docs.docker.com/engine/reference/commandline/tag/#extended-description
(defn tag [s]
  (when s (str/replace s #"[^a-zA-Z0-9_.-]" "_")))

(def repo "europe-west2-docker.pkg.dev/swirrl-devops-infrastructure-1/swirrl")

(defn- type-aliases
  "Returns the set of aliases which defines the build type in the options. If specified, the type
   should be either :pmd3 or :pmd4. The default is :pmd4."
  [opts]
  #{(:type opts :pmd4)})

(defn pmd4-docker-build [opts]
  (let [tags (->> ["rev-parse HEAD"
                   "describe --tags"
                   "branch --show-current"]
                  (map #(tag (b/git-process {:git-args %})))
                  (remove nil?))]
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
      :base-image "gcr.io/distroless/java17-debian11"
      :platforms #{:linux/amd64 :linux/arm64}
      ;; NOTE Not as documented!
      ;; The docstring states that these should be
      ;;     :to-registry {:username ... :password ...}
      ;; but alas, that is a lie.
      ;; https://github.com/juxt/pack.alpha/issues/101
      :to-registry-username "_json_key"
      :to-registry-password (System/getenv "GCLOUD_SERVICE_KEY")})))

(defn clean [_]
  (b/delete {:path "target"}))

(defn- jar-basis [opts]
  (b/create-basis {:project "deps.edn" :aliases (type-aliases opts)}))

(defn skinny
  "Builds a pack 'skinny' jar and a lib directory containing all dependencies"
  [_opts]
  (let [basis (b/create-basis {:project "deps.edn" :aliases [:skinny]})]
    (pack/skinny {:basis basis
                  :path "target/drafter.jar"
                  :path-coerce :jar
                  :libs "target/lib"
                  :lib-coerce :jar})))

(defn- copy-files [basis aliases]
  (let [alias-extra-paths (mapcat (fn [alias]
                                    (get-in basis [:aliases alias :extra-paths]))
                                  aliases)]
    (b/copy-dir {:src-dirs (concat ["src" "resources"] alias-extra-paths)
                 :target-dir class-dir})))

(defn jar
  "Builds a jar in the target directory"
  [opts]
  (clean opts)
  (let [basis (jar-basis opts)]
    (copy-files basis (type-aliases opts))

    (b/write-pom {:class-dir class-dir
                  :lib lib
                  :version version
                  :basis basis
                  :src-dirs ["src"]
                  :resource-dirs ["resources" "prod/clj/drafter"]})

    (b/jar {:class-dir class-dir
            :jar-file jar-file})))

(defn install
  "Builds a jar and publishes it to the local maven repository"
  [opts]
  (jar opts)
  (let [basis (jar-basis opts)]
    (b/install {:basis basis
                :lib lib
                :version version
                :jar-file jar-file
                :class-dir class-dir})))

(defn uber
  "Builds an uberjar in the target directory"
  [opts]
  (clean opts)

  (let [basis (jar-basis opts)]
    (copy-files basis (type-aliases opts))
    (b/compile-clj {:basis basis
                    :src-dirs ["src"]
                    :class-dir class-dir})
    (b/uber {:basis basis
             :class-dir class-dir
             :uber-file uberjar-file
             :main 'drafter.main})))
