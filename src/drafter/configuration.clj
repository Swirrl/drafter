(ns drafter.configuration
  "Namespace for calculating the SPARQL endpoint timeout settings. The
  timeout settings are represented as a complete two-level tree,
  where each level represents a facet of the endpoint timeouts. The
  first level is the endpoint name, then second whether it is a query
  or update endpoint. The leaves in the tree contain the
  corresponding timeout value. The tree is represented with nested
  maps e.g. for a single endpoint :live the tree will look like

  {:live {:update 100
          :query  100}}

  if the default timeouts are 100 for the entire operation.

  Every leaf timeout value in the tree has an associated path e.g.
  [:live :update] and every timeout configuration
  variable identifies a set of leaf nodes to update. A configuration
  setting consists of two parts - a 'selector' which defines the leaf
  nodes it affects, and a timeout value to update the leaves
  to. Applying a configuration setting therefore requires finding
  the set of leaf node paths corresponding to its selector and
  then updating their values.

  Finding the paths for a selector involves walking the configuration
  tree and keeping track of all matching paths seen so far. Each step
  of the walk corresponds to one of the facets of the endpoint - name and
  update/query. If a selector does not define a value for a facet, then
  it is represented as nil and matches all subtrees at the current level.

  Calculating the endpoint timeout configuration therefore proceeds as
  follows:

  1. Find all timeout settings in the environment map
  2. Order all settings from least to most specific
  3. For each setting:
     * Find the set of leaf nodes it references in the tree
     * Update all matching leaf timeouts to match the setting
  "
  (:require [clojure.tools.logging :as log]
            [clojure.string :as string]
            [drafter.timeouts :as timeouts]
            [aero.core :as aero]
            [drafter.util :as util])
  (:import [java.util.concurrent TimeUnit]))

(def timeout-param-prefix "drafter-timeout")
(def timeout-param-pattern #"drafter-timeout(-(?<method>(update|query)))?(-endpoint-(?<endpoint>([a-z]+)))?")

(defn create-selector
  "A configuration 'selector' identifies a set of configuration leaf
  nodes in the timeout configuration tree which are to be modified by
  a timeout setting. Each node in the tree has an endpoint (.e.g live,
  raw, draft), a method (query or update endpoint) and a
  scope (always operation)."
  [endpoint method]
  {:pre [(contains? #{:update :query nil} method)]}
  {:endpoint endpoint :method method})

;match-timeout-selector :: Keyword -> Try[Selector]
(defn match-timeout-selector
  "Parses a keyword into a timeout selector according to the timeout
  parameter format. If the keyword is not in the expected format, an
  Exception object is returned describing the error."
  [p]
  (let [matcher (re-matcher timeout-param-pattern (name p))]
    (if (.matches matcher)
      (let [endpoint (keyword (.group matcher "endpoint"))
            method (keyword (.group matcher "method"))]
        (create-selector endpoint method))
      (Exception. (str "Invalid format for drafter timeout variable '" p "'")))))

;selector->path :: Selector -> Vector[Keyword]
(defn- selector->path
  "Converts a selector into a path in the timeout settings tree."
  [s]
  ((juxt :endpoint :method) s))

(defn selector-lt?
  "Returns whether a selector is 'less than' (i.e. less specific) than
  another."
  [s1 s2]
  (neg? (compare (selector->path s1) (selector->path s2))))

;lift-f :: (Any* -> a) -> Try[Any]* -> Try[a]
(defn- lift-f [f & values]
  (if-let [ex (first (filter #(instance? Exception %) values))]
    ex
    (apply f values)))

;map-error-msg :: Try[a] -> (String -> String) -> Try[a]
(defn- map-error-msg [f v]
  (if (instance? Exception v)
    (Exception. (f (.getMessage v))))
  v)

(defn create-setting
  "A timeout setting contains a selector for a set of nodes in the
  timeout config tree along with a timeout value to use for those
  nodes."
  [selector timeout]
  {:timeout timeout :selector selector})

;parse-timeout-setting :: String -> String -> Try[Setting]
(defn- parse-timeout-setting [name value]
  (let [selector (match-timeout-selector name)
        timeout (->> (timeouts/try-parse-timeout value)
                     (map-error-msg (fn [m] (str "Invalid value for timeout parameter '" name "': " m))))]
    (lift-f create-setting selector timeout)))

;format-selector :: Selector -> String
(defn- format-selector [s]
  (let [pairs (map (fn [k] [k (k s)]) [:endpoint :method])
        segments (map (fn [[aspect value]] (str (name aspect) " => " (string/upper-case (name (or value :any))))) pairs)]
    (string/join ", " segments)))

;validate-setting-endpoint :: Set[Endpoint] -> Setting -> Try[Setting]
(defn- validate-setting-endpoint [endpoints {:keys [selector] :as setting}]
  (let [ep (:endpoint selector)]
    (if (or (nil? ep) (contains? endpoints ep))
      setting
      (Exception. (str "Unknown endpoint '" (name ep) "' for setting: " (format-selector selector))))))

;parse-and-validate-timeout-setting :: String -> String -> Set[Endpoint] -> Try[Setting]
(defn parse-and-validate-timeout-setting
  "Parses the selector and timeout value which specifies a timeout
  setting and validates that it can be applied to the timeout
  configuration."
  [name value endpoints]
  (->> (parse-timeout-setting name value)
       (lift-f #(validate-setting-endpoint endpoints %))))

(defn- looks-like-timeout-parameter?
  "Whether a pair looks like a definition for a timeout setting."
  [[k _]]
  (.startsWith (name k) timeout-param-prefix))

;find-timeout-variables :: Map[String, String] -> Set[Endpoint] -> {errors :: [Exception], settings :: [Setting]}
(defn find-timeout-variables
  "Finds all the timeout settings for a collection of endpoint in an
  environment map. Returns a map containing the valid settings, along
  with a list of Exception instances representing all the invalid
  settings found."
  [env endpoints]
  (let [timeout-vars (filter looks-like-timeout-parameter? env)
        validated (map (fn [[name value]] (parse-and-validate-timeout-setting name value endpoints)) timeout-vars)]
    (group-by #(if (instance? Exception %) :errors :settings) validated)))

(defn- create-initial-timeouts
  "Creates the initial configuration tree given a set of endpoints and
  the default timeouts."
  [endpoints default-timeouts]
  (let [default-m {:update default-timeouts :query default-timeouts}]
    (into {} (map vector endpoints (repeat default-m)))))

;match-nodes :: k -> Map k a -> [(k, a)]
(defn- match-nodes
  "Finds all the matching pairs in a map for the current stage
  'key'. If the key is nil then it matches all pairs. If it is non-nil
  then it returns the single matching pair if it exists in the source
  map. If it does not exist then nil is returned to indicate failure."
  [k m]
  (cond (nil? k) (vec m)
        (contains? m k) [[k (get m k)]]
        :else nil))

;step :: [Key] -> Key -> Tree a -> [(Path, Tree a)]
(defn step
  "Attempts to traverse a level inside the configuration tree given
  the current path in the tree and the key for the current
  level. Returns the set of all matching sub-paths inside the tree or
  nil if no matching path exists."
  [path stage tree]
  (if-let [sub-trees (match-nodes stage tree)]
    (mapv (fn [[node sub-tree]] [(conj path node) sub-tree]) sub-trees)))

;step-all :: [(Path, Tree a)] -> Key -> [(Path, Tree a)]
(defn- step-all
  "Traverses all paths for the current step selector value."
  [positions stage]
  (mapcat (fn [[path tree]] (step path stage tree)) positions))

;find-paths :: Tree a -> [Key] -> [(Path, a)]
(defn find-paths
  "Finds all paths inside a configuration tree which match a selector
  specification."
  [source-tree spec]
  (loop [positions [[[] source-tree]]
         to-match spec]
    (if-not (empty? to-match)
      (recur (step-all positions (first to-match)) (rest to-match))
      positions)))

;update-config-paths :: Tree a -> [(Path, a)] -> Tree a
(defn update-paths
  "Updates a set of leaf nodes in a timeouts tree. The new leaf values
  are specified by a pair of (Path, Value) where Path identifies the
  leaf node to update, and Value is the new value for the leaf."
  [config paths]
  (reduce (fn [acc [p v]] (assoc-in acc p v)) config paths))

;update-config :: Tree a -> [Key] -> a -> Tree a
(defn- update-config
  "Updates a tree by updating the value for all leaf nodes matching
  the given path specification.

  (update-config {:a {:b {:c 1 :d 2}}} [:a :b :c] 10)
  {:a {:b {:c 10 :d 2}}}"
  [source-tree spec timeout]
  (if-let [paths (find-paths source-tree spec)]
    (let [updated-paths (mapv (fn [[path cur-val]] [path timeout]) paths)]
      (update-paths source-tree updated-paths))
    source-tree))

(defn- order-settings [settings]
  (sort-by :selector selector-lt? settings))

(defn- apply-setting
  "Applies a timeout setting to the current timeout settings tree."
  [timeouts {:keys [timeout selector]}]
  (log/info (str "Applying setting " (format-selector selector) " with timeout " timeout))
  (update-config timeouts (selector->path selector) timeout))

(defn- log-config-errors [errors]
  (doseq [ex errors]
    (log/warn "Timeout configuration:" (.getMessage ex))))

(defn get-timeout-config
  "Calculates the timeout configuration tree given a collection of
  endpoints, the default timeouts for all operations and the current
  environment map."
  ([env endpoints] (get-timeout-config env endpoints timeouts/default-query-timeout))
  ([env endpoints default-timeouts]
   (let [default-config (create-initial-timeouts endpoints default-timeouts)
         endpoint-set (set endpoints)
         {:keys [errors settings]} (find-timeout-variables env endpoint-set)
         ordered-params (order-settings settings)]
     (log-config-errors errors)
     (reduce apply-setting default-config ordered-params))))

(defn get-endpoint-timeout
  "Gets the timeout for the endpoint type (query or update) on the
  named endpoint."
  [name endpoint-type timeout-config]
  {:pre [(#{:query :update} endpoint-type)]}
  (get-in timeout-config [name endpoint-type]))

(defmethod aero/reader 'timeout
  [opts tag value]
  (when (some? value)
    (let [timeout-or-ex (timeouts/try-parse-timeout value)]
      (if (util/throwable? timeout-or-ex)
        timeout-or-ex
        timeout-or-ex))))

(defmethod aero/reader 'port
  [opts tag value]
  (when (some? value)
    (try
      (let [port (Long/parseLong value)]
        (if (pos? port)
          port
          (IllegalArgumentException. "Port must be in range (0 65536]")))
      (catch Exception ex
        ex))))

(defn validate-configuration! [config]
  (let [invalid (filter (fn [[k v]] (util/throwable? v)) config)]
    (when-not (empty? invalid)
      (doseq [[key value] invalid]
        (.println *err* (format "Invalid value for configuration setting '%s': %s" (name key) (.getMessage value))))
      (let [msg (string/join (map #(.getMessage (second %)) invalid))]
        (throw (RuntimeException. (str "Configuration invalid: " msg)))))))

(defn get-configuration []
  (let [config (aero/read-config "config-settings.edn")]
    (validate-configuration! config)
    config))