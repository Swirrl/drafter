(ns drafter.stasher.filecache-test
  (:require [drafter.stasher.filecache :as sut]
            [clojure.test :as t]
            [me.raynes.fs :as fs]
            [clojure.core.cache :as cache]))

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
    ;; Note hash-maps are unordered, so they print differently
    ;; depending on their construction order.  This essentially tests
    ;; the implementation sorts the keys before generating an md5
    ;; hash.
    (t/is (= (sut/cache-key->hash-key {:b 2
                                       :a 1
                                       :d 4
                                       :c 3
                                       })

             (sut/cache-key->hash-key {:a 1
                                       :b 2
                                       :c 3
                                       :d 4})

             "0cc23d1cefa62b120c5f3b289503c01e"))))

(defn create-temp-file! []
  (java.io.File/createTempFile "drafter.stasher.filecache-test" ".tmp"))

(t/deftest lookup-file-cache
  (let [cache (sut/file-cache-factory {})]
    (t/is (nil? (cache/lookup cache {:an :un :cached :key })))
    (t/is (= :not-found (cache/lookup cache {:an :un :cached :key } :not-found)))


    (t/testing "Add & retrieve file from cache"
      
      (let [temp-file (create-temp-file!)
            cache (assoc cache {:cache :me} temp-file)])

      (t/is (instance? java.io.File (cache/lookup cache {:cache :me}))
            "File should be in cache under the {:cache :me} key")

      (t/testing "Evict key from cache"
        (t/is (nil? (-> (cache/evict cache {:cache :me})
                        (cache/lookup {:cache :me}))))))))
