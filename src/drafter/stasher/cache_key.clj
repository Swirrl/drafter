(ns drafter.stasher.cache-key
  (:require [clojure.spec.alpha :as s]))


(s/def ::livemod inst?)
(s/def ::draftmod inst?)
(s/def ::default-graphs set?)
(s/def ::named-graphs set?)
(s/def ::dataset (s/keys :req-un [::default-graphs ::named-graphs]))
(s/def ::query-type #{:graph :tuple :boolean})
(s/def ::query-str string?)
(s/def ::modified-times (s/keys :opt-un [::livemod ::draftmod]))
(s/def ::cache-key
  (s/keys :req-un [::dataset ::query-type ::query-str]
          :opt-un [::modified-times]))

(defn static-component [cache-key]
  {:pre [(s/valid? ::cache-key cache-key)]}
  (-> (dissoc cache-key :modified-times)
      (update-in [:dataset] (comp sort (partial apply into) vals))
      sort))

(defn time-component [cache-key]
  {:pre [(s/valid? ::cache-key cache-key)]}
  (let [{:keys [livemod draftmod]} (:modified-times cache-key)]
    (str (or (some-> livemod inst-ms) "empty")
         (and draftmod
              (str "-" (inst-ms draftmod))))))

(defn query-type [cache-key]
  (:query-type cache-key))

(comment

  (s/explain-str ::cache-key
                 {:dataset {:default-graphs #{}
                            :named-graphs #{}}
                  :query-type :tuple
                  :query-str "select adf"
                  :modified-times {:livemod #inst "2018-04-16T16:23:18.000-00:00"
                                   :draftmod #inst "2018-04-16T16:23:18.000-00:00"}})

  (s/explain-str ::cache-key
                 {:dataset {:default-graphs #{}
                            :named-graphs #{}}
                  :query-type :tuple
                  :query-str "select adf"
                  :modified-times {}})



  )



