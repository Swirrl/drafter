(ns drafter.stasher.cache-key
  (:require [clojure.spec.alpha :as s]
            [clojure.spec.gen.alpha :as g]
            [clojure.string :as str])
  (:import [java.time OffsetDateTime]))

(extend-protocol Inst
  java.time.OffsetDateTime
  (inst-ms* [this]
    (.. this toInstant toEpochMilli)))

(s/def ::datetime-with-tz (s/with-gen #(instance? OffsetDateTime %)
                            #(g/fmap
                              (fn [inst]
                                (.atOffset (.toInstant inst) java.time.ZoneOffset/UTC))
                              (s/gen inst?))) )

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

(s/def ::livemod ::datetime-with-tz)
(s/def ::draftmod ::datetime-with-tz)

(s/def ::default-graphs ::uri-set)
(s/def ::named-graphs ::uri-set)

(s/def ::dataset (s/keys :req-un [::default-graphs ::named-graphs]))
(s/def ::query-type #{:graph :tuple :boolean})
(s/def ::query-str string?)
(s/def ::modified-times (s/keys :opt-un [::livemod ::draftmod]))

(s/def ::modified-time ::datetime-with-tz)

(s/def ::cache-key
  (s/keys :req-un [::dataset ::query-type ::query-str ::modified-times]))

(s/def ::state-graph-cache-key
  (s/keys :req-un [::dataset ::query-type ::query-str ::modified-time]))

(s/def ::either-cache-key (s/or :cache-key ::cache-key
                                :state-graph-cache-key ::state-graph-cache-key))

(defn static-component [cache-key]
  (-> (dissoc cache-key :modified-times :modified-time)
      (update :dataset (comp (partial sort-by str) (partial apply into) vals))
      sort))

(s/fdef static-component
  :args (s/cat :cache-key ::either-cache-key))

(defn- key-type [key]
  (or (and (contains? key :modified-times) ::cache-key)
      (and (contains? key :modified-time) ::state-graph-cache-key)))

(defmulti time-component key-type)

(defmethod time-component ::cache-key [cache-key]
  (let [{:keys [livemod draftmod]} (:modified-times cache-key)]
    (str (or (some-> livemod inst-ms) "empty")
         (and draftmod
              (str "-" (inst-ms draftmod))))))

(defmethod time-component ::state-graph-cache-key [cache-key]
  (str (inst-ms (:modified-time cache-key))))

(s/def ::cache-key-time-component (s/or :empty-with-draft-mod #(re-matches #"(empty-)?\-?[0-9]+" %)
                                        :draft-and-live-mod #(re-matches #"\-?[0-9]+\-\-?[0-9]+" %)
                                        :empty (fn [s]
                                                 (str/starts-with? s "empty"))))

(s/def ::time-component (s/or :cache-key-time-component ::cache-key-time-component
                              :state-graph-time-component #(re-matches #"\-?[0-9]+" %)))

(s/fdef time-component
  :args (s/cat :cache-key ::either-cache-key)
  :ret ::time-component)


(defn query-type [cache-key]
  (:query-type cache-key))

(comment

  (clojure.spec.test.alpha/summarize-results (clojure.spec.test.alpha/check `time-component {:clojure.spec.test.check/opts {:num-tests 100}}))


  (s/explain-str ::cache-key
                 {:dataset {:default-graphs #{}
                            :named-graphs #{}}
                  :query-type :tuple
                  :query-str "select adf"
                  :modified-times {:livemod  (OffsetDateTime/parse "2018-04-16T16:23:18.000-00:00")
                                   :draftmod (OffsetDateTime/parse "2018-04-16T16:23:18.000-00:00")}})

  (s/explain-str ::cache-key
                 {:dataset {:default-graphs #{}
                            :named-graphs #{}}
                  :query-type :tuple
                  :query-str "select adf"
                  :modified-times {}})

  (s/explain-str ::state-graph-cache-key
                 {:dataset {:default-graphs #{}
                            :named-graphs #{}}
                  :query-type :tuple
                  :query-str "select adf"
                  :modified-time (OffsetDateTime/parse "2018-04-16T16:23:18.000-00:00")})


  (time-component {:dataset {:default-graphs #{}
                             :named-graphs #{}}
                   :query-type :tuple
                   :query-str "select adf"
                   :modified-time (OffsetDateTime/parse "2018-04-16T16:23:18.000-00:00")})


  (time-component {:dataset {:default-graphs #{}
                             :named-graphs #{}}
                   :query-type :tuple
                   :query-str "select adf"
                   :modified-times {:livemod (OffsetDateTime/parse "2018-04-16T16:23:18.000-00:00")
                                    :draftmod (OffsetDateTime/parse "2018-04-16T16:23:18.000-00:00")}})

  ;; (ns-unmap *ns* 'time-component)



  )
