(ns drafter.stasher.cache-key
  (:require [clojure.spec.alpha :as s]
            [clojure.spec.gen.alpha :as g]
            [drafter.util :as util])
  (:import [java.time OffsetDateTime]
           java.net.URI))

(extend-protocol Inst
  java.time.OffsetDateTime
  (inst-ms* [this]
    (.. this toInstant toEpochMilli)))

(s/def ::datetime-with-tz
  (s/with-gen #(instance? OffsetDateTime %)
    #(g/fmap
      (fn [inst]
        (.atOffset (.toInstant inst) java.time.ZoneOffset/UTC))
      (s/gen (s/and inst? (fn [t] (< 0 (.getTime t))))))) )

(s/def ::uri-string (s/with-gen string?
                      #(g/fmap
                        (partial str "http://")
                        (g/not-empty (g/string-alphanumeric)))))

(s/def ::java-uri (s/with-gen
                    #(instance? java.net.URI %)
                    #(g/fmap (fn [s]
                               (java.net.URI. s))
                             (s/gen ::uri-string))))

(s/def ::rdf4j-uri (s/with-gen
                     #(instance? org.eclipse.rdf4j.model.URI %)
                     #(g/fmap (fn [s]
                                (org.eclipse.rdf4j.model.impl.URIImpl. s))
                              (s/gen ::uri-string))))

(s/def ::uri-set (s/or :uri-strings (s/coll-of ::uri-string)
                       :java-uris (s/coll-of ::java-uri)
                       :rdf4j-uris (s/coll-of ::rdf4j-uri)))

(s/def ::version
  (s/with-gen
    (s/and ::java-uri
           #(re-matches
             #"http://publishmydata.com/def/drafter/version/[0-9a-f-]+"
             (str %)))
    #(g/fmap
      (fn [uuid]
        (URI. (str "http://publishmydata.com/def/drafter/version/" uuid)))
      (g/uuid))))

(s/def ::time ::datetime-with-tz)

(s/def ::livemod ::time)
(s/def ::draftmod ::time)

(s/def ::default-graphs ::uri-set)
(s/def ::named-graphs ::uri-set)

(s/def ::dataset (s/keys :req-un [::default-graphs ::named-graphs]))
(s/def ::query-type #{:graph :tuple :boolean})
(s/def ::query-str string?)
(s/def ::last-modified (s/keys :opt-un [::livemod ::draftmod ::version]))
(s/def ::state-graph-last-modified (s/keys :req-un [::time ::version]))

(s/def ::cache-key
  (s/keys :req-un
          [::dataset ::query-type ::query-str ::last-modified]))

(s/def ::state-graph-cache-key
  (s/keys :req-un
          [::dataset ::query-type ::query-str ::state-graph-last-modified]))

(s/def ::either-cache-key (s/or :cache-key ::cache-key
                                :state-graph-cache-key ::state-graph-cache-key))

(defn- deep-sort
  "Recursively sort maps and sets"
  [x]
  (cond
    (map? x) (sort (map (fn [[k v]] [k (deep-sort v)]) x))
    (set? x) (sort (map deep-sort x))
    (coll? x) (map deep-sort x)
    :else x))

(defn static-component [cache-key]
  (-> (dissoc cache-key :last-modified :state-graph-last-modified)
    deep-sort))

(s/fdef static-component
  :args (s/cat :cache-key ::either-cache-key))

(defn- key-type [key]
  (or (and (contains? key :last-modified) ::cache-key)
      (and (contains? key :state-graph-last-modified) ::state-graph-cache-key)))

(defmulti time-component key-type)

(defmethod time-component ::cache-key
  [{{:keys [livemod draftmod version]} :last-modified}]
  (str (or (some-> livemod inst-ms) "empty")
       (when draftmod
         (str "-" (inst-ms draftmod)))
       (when version
         (str "_" (util/version->str version)))))

(defmethod time-component ::state-graph-cache-key
  [{{:keys [time version]} :state-graph-last-modified}]
  (str (inst-ms time)
       "_"
       (util/version->str version)))

(s/def ::time-component
  #(re-matches #"(empty|[0-9]+)(-[0-9]+)?(_[0-9a-f-]+){0,2}" %))

(s/fdef time-component
  :args (s/cat :cache-key ::either-cache-key)
  :ret ::time-component)


(defn query-type [cache-key]
  (:query-type cache-key))

(comment

  (clojure.spec.test.alpha/summarize-results
   (clojure.spec.test.alpha/check
    `time-component {:clojure.spec.test.check/opts {:num-tests 100}}))


  (s/explain-str ::cache-key
                 {:dataset {:default-graphs #{}
                            :named-graphs #{}}
                  :query-type :tuple
                  :query-str "select adf"
                  :last-modified {:livemod  (OffsetDateTime/parse "2018-04-16T16:23:18.000-00:00")
                                  :draftmod (OffsetDateTime/parse "2018-04-16T16:23:18.000-00:00")
                                  :version (util/version)}})

  (s/explain-str ::cache-key
                 {:dataset {:default-graphs #{}
                            :named-graphs #{}}
                  :query-type :tuple
                  :query-str "select adf"
                  :last-modified {}})

  (s/explain-str ::state-graph-cache-key
                 {:dataset {:default-graphs #{}
                            :named-graphs #{}}
                  :query-type :tuple
                  :query-str "select adf"
                  :state-graph-last-modified {:time (OffsetDateTime/parse "2018-04-16T16:23:18.000-00:00")
                                              :version (util/version)}})


  (time-component {:dataset {:default-graphs #{}
                             :named-graphs #{}}
                   :query-type :tuple
                   :query-str "select adf"
                   :state-graph-last-modified {:time (OffsetDateTime/parse "2018-04-16T16:23:18.000-00:00")
                                               :version (util/version)}})


  (time-component {:dataset {:default-graphs #{}
                             :named-graphs #{}}
                   :query-type :tuple
                   :query-str "select adf"
                   :last-modified {:livemod (OffsetDateTime/parse "2018-04-16T16:23:18.000-00:00")
                                   :draftmod (OffsetDateTime/parse "2018-04-16T16:23:18.000-00:00")
                                   :version (util/version)}})

  ;; (ns-unmap *ns* 'time-component)



  )
