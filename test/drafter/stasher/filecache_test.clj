(ns drafter.stasher.filecache-test
  (:require [drafter.stasher.filecache :as sut]
            [clojure.test :as t]
            [me.raynes.fs :as fs]
            [clojure.core.cache :as cache]
            [clojure.java.io :as io]
            [clojure.spec.test.alpha :as st]
            [clojure.spec.alpha :as s]
            [clojure.spec.gen.alpha :as sg]))

(def test-path (fs/file "tmp" "filecache-test"))

(defn with-temp-directory [t]
  (try
    (fs/mkdirs test-path)
    (t)
    (finally
      (fs/delete-dir test-path))))

(t/use-fixtures :once with-temp-directory)

(t/deftest cache-key->hashed-key-test
  (t/testing "Same values should generate same hash"
    (let [cache (sut/file-cache-factory {:dir "tmp/test-stasher-cache"
                                         :backend-rdf-format :test})
          key {:dataset {:default-graphs #{"http://graphs/2"
                                           "http://graphs/1"
                                           "http://graphs/3"
                                           "http://graphs/4"
                                           "http://graphs/5"},
                         :named-graphs #{"http://graphs/3"
                                         "http://graphs/4"
                                         "http://graphs/5"
                                         "http://graphs/6"
                                         "http://graphs/7"
                                         "http://graphs/8"}},
               :query-type :graph,
               :query-str "7ACswxwR95kCP743"
               :modified-times {:livemod #inst "2018-01-01T10:03:18.000-00:00"
                                :draftmod #inst "2018-04-16T16:23:18.000-00:00"}}]
      ;; Note hash-maps are unordered, so they print differently
      ;; depending on their construction order.  This essentially tests
      ;; the implementation sorts the keys before generating an md5
      ;; hash.
      (t/is (= (sut/cache-key->cache-path cache key)
               (sut/cache-key->cache-path cache (-> key
                                                    (dissoc :query-type)
                                                    (assoc :query-type :graph)))
               (sut/cache-key->cache-path cache (-> key
                                                    (update-in [:dataset :default-graphs]
                                                               (comp set shuffle))))
               (sut/cache-key->cache-path cache (-> key
                                                    (update :modified-times dissoc :livemod)
                                                    (assoc-in [:modified-times :livemod]
                                                              #inst "2018-01-01T10:03:18.000-00:00")))
               (sut/cache-key->cache-path cache
                                          (into {} (map (fn [[k v]] (if (map? v)
                                                                     [k (into {} (shuffle (vec v)))]
                                                                     [k v]))
                                                        key))))))))


(defn create-temp-file! []
  (java.io.File/createTempFile "drafter.stasher.filecache-test" ".tmp"))

(t/deftest lookup-file-cache
  (let [cache (sut/file-cache-factory {})]

    (t/testing "Assert validity of cache entries"
      (t/is (thrown? clojure.lang.ExceptionInfo
                     (cache/miss cache {:foo :bar} (io/file test-path "test.file")))
            "Must contain a valid :query-type key, so we can serialise the data.")
      (t/is (thrown? clojure.lang.ExceptionInfo
                     (cache/miss cache {:query-type :invalid-query-type} (io/file test-path "test.file")))))
    
    (t/testing "Not found"
      (let [uncached-key {:query-type :tuple
                          :query-str "an uncached query"
                          :dataset {:default-graphs #{"http://graphs/test-graph"}
                                    :named-graphs #{}}
                          :modified-times {:livemod #inst "2017-02-02T02:02:02.000-00:00"}}]
        (t/is (nil? (cache/lookup cache uncached-key)))
        (t/is (= :not-found (cache/lookup cache uncached-key :not-found)))))


    (t/testing "Add & retrieve file from cache"
      (let [temp-file (create-temp-file!)
            cache-key {:query-type :tuple
                       :query-str "cache me"
                       :dataset {:default-graphs #{"http://graphs/test-graph"}
                                 :named-graphs #{}}
                       :modified-times {}}
            cache (assoc cache cache-key temp-file)]

        (t/is (instance? java.io.File (cache/lookup cache cache-key))
              "File should be in cache under the {:cache :me} key")

        (t/testing "Evict key from cache"
          (t/is (nil? (-> (cache/evict cache cache-key)
                          (cache/lookup cache-key)))))))))

(t/deftest store-last-modified-on-cache-hit
  ;; These test require a few workarounds for filesystems that do not store ms
  ;; precision on the last modified stamp.
  (let [cache (sut/file-cache-factory {:dir "tmp/test-stasher-cache"
                                       :backend-rdf-format :test})
        ->sec (fn [ms]
                (.toSeconds java.util.concurrent.TimeUnit/MILLISECONDS ms))]
    (t/testing "Cache miss sets last access to now (more or less)"
      (let [cache-key {:query-type :tuple
                       :query-str "stored entries have a last access set"
                       :dataset {:default-graphs #{"http://graphs/test-graph"}
                                 :named-graphs #{}}
                       :modified-times {:livemod #inst "2017-02-02T02:02:02.000-00:00"}}]
        (let [before-ms (System/currentTimeMillis)
              _ (cache/miss cache cache-key (create-temp-file!))
              last-access-ms (sut/last-accessed-time (sut/cache-key->cache-path cache cache-key))
              after-ms (System/currentTimeMillis)]
          (t/is (<= (->sec before-ms)
                    (->sec last-access-ms)
                    (->sec after-ms))))))
    (t/testing "Cache hit bumps the last access time"
      (let [cache-key {:query-type :tuple
                       :query-str "cache hit bumps access time"
                       :dataset {:default-graphs #{"http://graphs/test-graph"}
                                 :named-graphs #{}}
                       :modified-times {:livemod #inst "2017-02-02T02:02:02.000-00:00"}}
            fake-last-access-time (- (System/currentTimeMillis) 10000)]
        (cache/miss cache cache-key (create-temp-file!))
        (fs/touch (sut/cache-key->cache-path cache cache-key) fake-last-access-time)
        (t/is (= (->sec fake-last-access-time)
                 (->sec (sut/last-accessed-time (sut/cache-key->cache-path cache cache-key)))))
        (cache/hit cache cache-key)
        (t/is (< fake-last-access-time
                 (sut/last-accessed-time (sut/cache-key->cache-path cache cache-key))))))
    (t/testing "Cache lookup bumps the last access time"
      (let [cache-key {:query-type :tuple
                       :query-str "cache lookup bumps access time"
                       :dataset {:default-graphs #{"http://graphs/test-graph"}
                                 :named-graphs #{}}
                       :modified-times {:livemod #inst "2017-02-02T02:02:02.000-00:00"}}
            fake-last-access-time (- (System/currentTimeMillis) 10000)]
        (cache/miss cache cache-key (create-temp-file!))
        (fs/touch (sut/cache-key->cache-path cache cache-key) fake-last-access-time)
        (t/is (= (->sec fake-last-access-time)
                 (->sec (sut/last-accessed-time (sut/cache-key->cache-path cache cache-key)))))
        (cache/lookup cache cache-key)
        (t/is (< fake-last-access-time
                 (sut/last-accessed-time (sut/cache-key->cache-path cache cache-key))))))))
