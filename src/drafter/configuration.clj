(ns drafter.configuration
  (:require [taoensso.timbre :as timbre]
            [clojure.string :as string])
  (:import [java.util Comparator]
           [java.util.concurrent TimeUnit]))

(def timeout-param-prefix "drafter-timeout")
(def timeout-param-pattern #"drafter-timeout(-(?<method>(update|query)))?(-endpoint-(?<endpoint>([a-z]+)))?(-(?<scope>(result|operation)))?")

(defn create-selector [endpoint method scope]
  {:pre [(contains? #{:update :query nil} method)
         (contains? #{:result-timeout :operation-timeout nil} scope)]}
  {:endpoint endpoint :method method :scope scope})

(def selector-endpoint :endpoint)
(def selector-method :method)
(def selector-scope :scope)

(defn match-timeout-selector [p]
  (let [matcher (re-matcher timeout-param-pattern (name p))]
    (if (.matches matcher)
      (let [endpoint (keyword (.group matcher "endpoint"))
            method (keyword (.group matcher "method"))
            scope-group (.group matcher "scope")
            scope (if (nil? scope-group) nil (keyword (str scope-group "-timeout")))]
        (create-selector endpoint method scope))
      (Exception. (str "Invalid format for drafter timeout variable '" p "'")))))

(defn selector->path [s]
  ((juxt :endpoint :method :scope) s))

(defn selector-lt? [s1 s2]
  (< (compare (selector->path s1) (selector->path s2)) 0))

(defn lift-f [f & values]
  (if-let [ex (first (filter #(instance? Exception %) values))]
    ex
    (apply f values)))

(defn map-error-msg [f v]
  (if (instance? Exception v)
    (Exception. (f (.getMessage v))))
  v)

(defn try-parse-timeout [s]
  (try
    (let [timeout (Integer/parseInt s)]
      (if (> timeout 0)
        (.toMillis TimeUnit/SECONDS timeout)
        (Exception. "Timeout values must be non-negative")))
    (catch NumberFormatException ex
      (Exception. (str "Timeout value '" s "' is not an integer")))))

(defn create-setting [selector timeout]
  {:timeout timeout :selector selector})

(defn parse-timeout-setting [name value]
  (let [selector (match-timeout-selector name)
        timeout (->> (try-parse-timeout value)
                     (map-error-msg (fn [m] (str "Invalid value for timeout parameter '" name "': " m))))]
    (lift-f create-setting selector timeout)))

;validate-setting-endpoint :: Set[Endpoint] -> Setting -> Try[Setting]
(defn validate-setting-endpoint [endpoints {:keys [selector] :as setting}]
  (let [ep (selector-endpoint selector)]
    (if (or (nil? ep) (contains? endpoints ep))
      setting
      (Exception. (str "Found setting for unknown endpoint '" (name (selector-endpoint selector)) "'")))))

(defn parse-and-validate-timeout-setting [name value endpoints]
  (->> (parse-timeout-setting name value)
       (lift-f #(validate-setting-endpoint endpoints %))))

(defn looks-like-timeout-parameter? [[k _]]
  (.startsWith (name k) timeout-param-prefix))

;find-timeout-variables :: Map[String, String] -> Set[Endpoint] -> {errors :: [Exception], settings :: [Setting]}
(defn find-timeout-variables [env endpoints]
  (let [timeout-vars (filter looks-like-timeout-parameter? env)
        validated (map (fn [[name value]] (parse-and-validate-timeout-setting name value endpoints)) timeout-vars)]
    (group-by #(if (instance? Exception %) :errors :settings) validated)))

(defn create-initial-timeouts [endpoints default-timeouts]
  (let [default-m {:update default-timeouts :query default-timeouts}]
    (into {} (map vector endpoints (repeat default-m)))))

;match-nodes :: k -> Map k a -> [(k, a)]
(defn match-nodes [k m]
  (cond (nil? k) (vec m)
        (contains? m k) [[k (get m k)]]
        :else nil))

;step :: [Key] -> Key -> Tree a -> [(Path, Tree a)]
(defn step [path stage tree]
  (if-let [sub-trees (match-nodes stage tree)]
    (mapv (fn [[node sub-tree]] [(conj path node) sub-tree]) sub-trees)))

;step-all :: [(Path, Tree a)] -> Key -> [(Path, Tree a)]
(defn step-all [positions stage]
  (mapcat (fn [[path tree]] (step path stage tree)) positions))

;find-paths :: Tree a -> [Key] -> [(Path, a)]
(defn find-paths [source-tree spec]
  (loop [positions [[[] source-tree]]
         to-match spec]
    (if-not (empty? to-match)
      (recur (step-all positions (first to-match)) (rest to-match))
      positions)))

;update-config-paths :: Tree a -> [(Path, a)] -> Tree a
(defn update-paths [config paths]
  (reduce (fn [acc [p v]] (assoc-in acc p v)) config paths))

;config-tree :: Tree a -> [Key]  -> a -> Tree a
(defn update-config [source-tree spec timeout]
  (if-let [paths (find-paths source-tree spec)]
    (let [updated-paths (mapv (fn [[path cur-val]] [path timeout]) paths)]
      (update-paths source-tree updated-paths))
    source-tree))

(defn order-settings [settings]
  (sort-by :selector selector-lt? settings))

(defn format-selector [s]
  (let [pairs (map (fn [k] [k (k s)]) [:endpoint :method :scope])
        segments (map (fn [[aspect value]] (str (name aspect) " => " (string/upper-case (name (or value :any))))) pairs)]
    (string/join ", " segments)))

(defn apply-setting [timeouts {:keys [timeout selector]}]
  (timbre/info (str "Applying setting " (format-selector selector) " with timeout " timeout))
  (update-config timeouts (selector->path selector) timeout))

(defn log-config-errors [errors]
  (doseq [ex errors]
    (timbre/warn "Timeout configuration:" (.getMessage ex))))

(defn get-timeout-config [env endpoints default-timeouts]
  (let [default-config (create-initial-timeouts endpoints default-timeouts)
        endpoint-set (set endpoints)
        {:keys [errors settings]} (find-timeout-variables env endpoint-set)
        ordered-params (order-settings settings)]
    (log-config-errors errors)
    (reduce apply-setting default-config ordered-params)))

