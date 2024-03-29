(ns drafter.stasher.filecache-test
  (:require
   [clojure.test :as t]
   [drafter.stasher.filecache :as sut]
   [drafter.test-common :as tc]
   [drafter.util :as util]
   [me.raynes.fs :as fs])
  (:import java.time.OffsetDateTime))

(t/use-fixtures :each tc/with-spec-instrumentation)

(def test-path (fs/file "tmp" "filecache-test"))

(defn with-temp-directory [t]
  (try
    (fs/mkdirs test-path)
    (t)
    (finally
      (fs/delete-dir test-path))))

(t/use-fixtures :each with-temp-directory)

(t/deftest cache-key->hashed-key-test
  (t/testing "Same values should generate same hash"
    (let [dir "tmp/test-stasher-cache"
          ext :nt
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
               :last-modified {:livemod (OffsetDateTime/parse "2018-01-01T10:03:18.000-00:00")
                               :draftmod (OffsetDateTime/parse "2018-04-16T16:23:18.000-00:00")
                               :version (util/version "bc0bc9e6-c494-4014-be8e-89073f40bb61")}}]
      ;; Note hash-maps are unordered, so they print differently
      ;; depending on their construction order.  This essentially tests
      ;; the implementation sorts the keys before generating an md5
      ;; hash.
      (t/is (= (sut/cache-key->cache-path dir ext key)
               (sut/cache-key->cache-path dir
                                          ext
                                          (assoc key :query-type :graph))
               (sut/cache-key->cache-path dir
                                          ext
                                          (update-in key
                                                     [:dataset :default-graphs]
                                                     (comp set shuffle)))
               (sut/cache-key->cache-path dir
                                          ext
                                          (assoc-in
                                           key
                                           [:last-modified :livemod]
                                           (OffsetDateTime/parse
                                            "2018-01-01T10:03:18.000-00:00")))
               (sut/cache-key->cache-path
                dir
                ext
                (assoc-in key
                          [:last-modified :version]
                          (util/version
                           "bc0bc9e6-c494-4014-be8e-89073f40bb61")))
               (sut/cache-key->cache-path dir
                                          ext
                                          (into
                                           {}
                                           (map
                                            (fn [[k v]]
                                              (if (map? v)
                                                [k (into {} (shuffle (vec v)))]
                                                [k v]))
                                            key)))))))

  (t/testing "Values that differ only in version should generate different hash"
    (let [dir "tmp/test-stasher-cache"
          ext :nt
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
               :last-modified {:livemod (OffsetDateTime/parse "2018-01-01T10:03:18.000-00:00")
                               :draftmod (OffsetDateTime/parse "2018-04-16T16:23:18.000-00:00")
                               :version (util/version "bc0bc9e6-c494-4014-be8e-89073f40bb61")}}]
      (t/is (not= (sut/cache-key->cache-path dir ext key)
                  (sut/cache-key->cache-path dir ext (assoc-in
                                                      key
                                                      [:last-modified :version]
                                                      (util/version))))))))

(t/deftest lookup-file-cache
  (let [cache (sut/make-file-backend {:dir test-path})
        fmt :srj]
    (t/testing "Searching for an entry not in the cache returns a nil source"
      (let [uncached-key {:query-type :tuple
                          :query-str "an uncached query"
                          :dataset {:default-graphs #{"http://graphs/test-graph"}
                                    :named-graphs #{}}
                          :last-modified {:livemod (OffsetDateTime/parse "2017-02-02T02:02:02.000-00:00")
                                          :version (util/version)}}]
        (t/is (nil? (sut/source-stream cache uncached-key :srj)))))))

(t/deftest writing-and-reading
  (t/testing "Add & retrieve file from cache"
    (let [cache (sut/make-file-backend {})
          fmt :json
          cache-key {:query-type :tuple
                     :query-str (str "cache me" (.nextInt (java.util.Random.) 100000000))
                     :dataset {:default-graphs #{"http://graphs/test-graph"}
                               :named-graphs #{}}
                     :last-modified {}}
          to-write (.getBytes "blablabla")]
      (with-open  [write-stream (sut/destination-stream cache cache-key fmt)]
        (.write write-stream to-write 0 9))
      (with-open [read-stream (sut/source-stream cache cache-key fmt)]
        (t/is read-stream "File should be in cache")
        (t/is (= (vec to-write)
                 (repeatedly 9 #(.read read-stream))))))))


(t/deftest store-last-modified-on-cache-hit
  ;; These test require a few workarounds for filesystems that do not store ms
  ;; precision on the last modified stamp.
  (let [dir "tmp/test-stasher-cache"
        fmt :brt ;; binary tuple format

        cache (sut/make-file-backend {:dir dir})
        ->sec (fn [ms]
                (.toSeconds java.util.concurrent.TimeUnit/MILLISECONDS ms))]
    (t/testing "Cache miss sets last access to now (more or less)"
      (let [cache-key {:query-type :tuple
                       :query-str "stored entries have a last access set"
                       :dataset {:default-graphs #{"http://graphs/test-graph"}
                                 :named-graphs #{}}
                       :last-modified {:livemod (OffsetDateTime/parse "2017-02-02T02:02:02.000-00:00")
                                       :version (util/version)}}]
        (let [before-ms (System/currentTimeMillis)
              _ (with-open [write-stream (sut/destination-stream cache cache-key fmt)]
                  (.write write-stream (.getBytes "hello") 0 5))
              file-on-disk (sut/cache-key->cache-path dir fmt cache-key)
              last-access-ms (sut/last-accessed-time file-on-disk)
              after-ms (System/currentTimeMillis)]
          (t/is (fs/exists? file-on-disk))
          (t/is (<= (->sec before-ms)
                    (->sec last-access-ms)
                    (->sec after-ms))))))
    (t/testing "Cache hit bumps the last access time"
      (let [cache-key {:query-type :tuple
                       :query-str "cache hit bumps access time"
                       :dataset {:default-graphs #{"http://graphs/test-graph"}
                                 :named-graphs #{}}
                       :last-modified {:livemod (OffsetDateTime/parse "2017-02-02T02:02:02.000-00:00")
                                       :version (util/version)}}
            fake-last-access-time (- (System/currentTimeMillis) 10000)]
        (with-open [write-stream (sut/destination-stream cache cache-key fmt)])
        (fs/touch (sut/cache-key->cache-path dir fmt cache-key) fake-last-access-time)
        (t/is (= (->sec fake-last-access-time)
                 (->sec (sut/last-accessed-time (sut/cache-key->cache-path dir fmt cache-key)))))
        (with-open [read-stream (sut/source-stream cache cache-key fmt)]
          (t/is (some? read-stream)))
        (t/is (< fake-last-access-time
                 (sut/last-accessed-time (sut/cache-key->cache-path dir fmt cache-key))))))
    (t/testing "Cache lookup bumps the last access time"
      (let [cache-key {:query-type :tuple
                       :query-str "cache lookup bumps access time"
                       :dataset {:default-graphs #{"http://graphs/test-graph"}
                                 :named-graphs #{}}
                       :last-modified {:livemod (OffsetDateTime/parse "2017-02-02T02:02:02.000-00:00")
                                       :version (util/version)}}
            fake-last-access-time (- (System/currentTimeMillis) 10000)]
        (with-open [write-stream (sut/destination-stream cache cache-key fmt)])
        (fs/touch (sut/cache-key->cache-path dir fmt cache-key) fake-last-access-time)
        (t/is (= (->sec fake-last-access-time)
                 (->sec (sut/last-accessed-time (sut/cache-key->cache-path dir fmt cache-key)))))
        (with-open [read-stream (sut/source-stream cache cache-key fmt)]
          (t/is (some? read-stream)))
        (t/is (< fake-last-access-time
                 (sut/last-accessed-time (sut/cache-key->cache-path dir fmt cache-key))))))))
