(ns drafter.stasher.cache-clearer-test
  (:require [drafter.stasher.cache-clearer :as sut]
            [clojure.java.io :as io]
            [me.raynes.fs :as fs]
            [clojure.test :as t])
  (:import org.apache.commons.codec.binary.Hex
           java.security.MessageDigest
           java.time.OffsetDateTime))

(t/deftest build-cache-entry-meta-data
  (t/testing "Cache of a draft"
    (t/is (= (-> (sut/->entry-meta-data "/cache/66/3b/663b62da5c46f6d1fece1a1a1b30fa63/1514800998000-1523895798000.test")
                 (select-keys [:hash :livemod :draftmod]))
             {:hash "663b62da5c46f6d1fece1a1a1b30fa63"
              :livemod 1514800998000
              :draftmod 1523895798000})))
  (t/testing "Cache of a live query"
    (t/is (= (-> (sut/->entry-meta-data "/cache/66/3b/663b62da5c46f6d1fece1a1a1b30fa63/1514800998000.test")
                 (select-keys [:hash :livemod :draftmod]))
             {:hash "663b62da5c46f6d1fece1a1a1b30fa63"
              :livemod 1514800998000
              :draftmod nil})))
  (t/testing "Cache of a query against no graph"
    (t/is (= (-> (sut/->entry-meta-data "/cache/66/3b/663b62da5c46f6d1fece1a1a1b30fa63/empty.test")
                 (select-keys [:hash :livemod :draftmod]))
             {:hash "663b62da5c46f6d1fece1a1a1b30fa63"
              :livemod nil
              :draftmod nil})))
  (t/testing "Invalid file in the cache"
    (t/is (contains? (sut/->entry-meta-data "/cache/THIS_FILE_DOES_NOT_MATCH_THE_FORMAT.WRONG_FILE")
                     :exception)))
  (t/testing "Meta data contains the last modified date"
    (let [cache-dir (fs/temp-dir "stasher/testing-modified-date")
          file (fs/file cache-dir "66" "3b" "663b62da5c46f6d1fece1a1a1b30fa63" "1514800998000-1523895798000.test")]
      (io/make-parents file)
      (fs/create file)
      (t/is (< 0 (:last-access (sut/->entry-meta-data file)))))))

(defn- add-to-cache
  "Sneak a file into the cache using the correct filename format"
  [cache-dir hash ?livemod ?draftmod]
  (let [f (fs/file cache-dir
                   (subs hash 0 2)
                   (subs hash 2 4)
                   hash
                   (str
                    (or ?livemod "empty")
                    (or (and ?draftmod (str "-" ?draftmod)) "")
                    ".test"))]
    (io/make-parents f)
    (fs/create f)))


(t/deftest finding-all-files
  (let [->hash   (fn [n] (let [md (MessageDigest/getInstance "MD5")]
                          (Hex/encodeHexString (.digest md (.getBytes (str n))))))]
    (t/testing "Finding files stored in the cache, normal usage"
      (let [cache-dir (fs/temp-dir "stasher/cache-clearer-test")
            no-of-files 10
            file-meta (map (juxt ->hash (partial + 1000) (partial + 2000))
                           (range no-of-files))
            _ (doseq [[hash livemod draftmod] file-meta]
                (add-to-cache cache-dir hash livemod draftmod))
            all-files (sut/all-files cache-dir)]
        (t/testing "Everything we put in is retrieved correctly"
          (t/is (= no-of-files
                   (count all-files)))
          (t/is (= (set file-meta)
                   (set (map (juxt :hash :livemod :draftmod) all-files)))
                "The retrieved files contain the same hash and mod times that we inserted"))))
    (t/testing "Rogue files stored in the cache are ignored"
      (let [cache-dir (fs/temp-dir "stasher/cache-clearer-test-rogue-files")]
        (t/is (empty? (sut/all-files cache-dir)))
        ;; Invalid file in root of cache
        (fs/create (fs/file cache-dir "ignore_this_file"))
        (let [valid-hash (->hash "valid-hash")]
          (add-to-cache cache-dir valid-hash "invalid live mod" nil)
          (add-to-cache cache-dir valid-hash "invalid live mod" 34533)
          (add-to-cache cache-dir valid-hash 1244 "invalid draft mod")
          (add-to-cache cache-dir valid-hash "invalid live mod" "invalid draft mod"))
        (t/is (empty? (sut/all-files cache-dir))
              "None of the invalid files are returned")))))


(t/deftest expired-files
  "An expired file is one that has a timestamp older than the latest version in
   the cache."
  (t/testing "All files with the same hash"
    (t/testing "Single live entry for hash is not expired"
      (let [files [{:hash "663b62da5c46f6d1fece1a1a1b30fa63"
                    :livemod 100
                    :draftmod nil}]]
        (t/is (empty? (sut/find-expired files)))))
    (t/testing "Single draft entry for hash is not expired"
      (let [files [{:hash "663b62da5c46f6d1fece1a1a1b30fa63"
                    :livemod 100
                    :draftmod 105}]]
        (t/is (empty? (sut/find-expired files)))))
    (t/testing "Old live version is expired"
      (let [new-version {:hash "663b62da5c46f6d1fece1a1a1b30fa63"
                         :livemod 9999999
                         :draftmod nil}
            old-version {:hash "663b62da5c46f6d1fece1a1a1b30fa63"
                         :livemod 1
                         :draftmod nil}
            files [new-version
                   old-version]]
        (t/is (= #{old-version}
                 (set (sut/find-expired files))))))
    (t/testing "New drafts expire old drafts"
      (let [old-version {:hash "663b62da5c46f6d1fece1a1a1b30fa63"
                         :livemod 100
                         :draftmod 105}
            new-draft-version {:hash "663b62da5c46f6d1fece1a1a1b30fa63"
                               :livemod 100
                               :draftmod 999}
            files [old-version
                   new-draft-version]]
        (t/is (= #{old-version}
                 (set (sut/find-expired files))))))
    (t/testing "Draft on newer version evicts older live versions"
      (let [old-version {:hash "663b62da5c46f6d1fece1a1a1b30fa63"
                         :livemod 1
                         :draftmod nil}
            new-draft-version {:hash "663b62da5c46f6d1fece1a1a1b30fa63"
                               :livemod 101
                               :draftmod 105}
            files [old-version
                   new-draft-version]]
        (t/is (= #{old-version}
                 (set (sut/find-expired files))))))
    (t/testing "If we have an empty livemod then class it as older than everything"
      ;; TODO Think about this behaviour. In what cases will we have an empty
      ;; livemod? And can we have an empty livemod and a valid livemod on the same
      ;; hash?
      (let [empty-livemod-version {:hash "663b62da5c46f6d1fece1a1a1b30fa63"
                                   :livemod nil
                                   :draftmod nil}
            livemod-version {:hash "663b62da5c46f6d1fece1a1a1b30fa63"
                             :livemod 100
                             :draftmod nil}
            files [empty-livemod-version
                   livemod-version]]
        (t/is (= #{empty-livemod-version}
                 (set (sut/find-expired files)))))))
  (t/testing "Many files with differenct hash"
    ;; A few of the prior tests, but with an extra file thrown in to try and
    ;; confused them
    (t/testing "Different files don't affect each other"
      (let [_2cd {:hash "2cd56d42a9ad5cb5d656155dd4b68663f4fc32"
                  :livemod 9999999
                  :draftmod nil}
            _663 {:hash "663b62da5c46f6d1fece1a1a1b30fa63"
                  :livemod 1
                  :draftmod nil}
            files [_2cd _663]]
        (t/is (empty? (sut/find-expired files)))))
    (t/testing "Different files don't affect each other"
      (let [_2cd {:hash "2cd56d42a9ad5cb5d656155dd4b68663f4fc32"
                  :livemod 9999999
                  :draftmod nil}
            _2cd-old {:hash "2cd56d42a9ad5cb5d656155dd4b68663f4fc32"
                      :livemod 1
                      :draftmod nil}
            _663 {:hash "663b62da5c46f6d1fece1a1a1b30fa63"
                  :livemod 1
                  :draftmod nil}
            files [_2cd _2cd-old _663]]
        (t/is (= #{_2cd-old}
                 (set (sut/find-expired files))))))))

(defn- gb->bytes [gb]
  (* gb 1024 1024 1024))


(t/deftest finding-old-files-to-delete
  "The find-old-files function accepts a list of files and an amount in bytes
   and will remove a minimum of this many bytes from the list"
  (t/testing "Basic functionality"
    (let [files [{:size-bytes (gb->bytes 1)}]]
      (t/is (= files
               (sut/find-old-files files 1))
            "Something is found for removal when bytes to remove is non-zero"))
    (let [files [{:size (gb->bytes 1)}]]
      (t/is (empty? (sut/find-old-files files 0))
            "Nothing is found for removal when bytes to remove is zero")))
  (t/testing "Prefer older files to newer files"
    (let [old-file {:size-bytes (gb->bytes 1)
                    :last-access (inst-ms (OffsetDateTime/parse "1970-01-01T00:00:00Z"))}
          new-file {:size-bytes (gb->bytes 1)
                    :last-access (inst-ms (OffsetDateTime/parse "2070-01-01T00:00:00Z"))}]
      (t/is (= [old-file]
               (sut/find-old-files [old-file new-file]
                                   (gb->bytes 1)))
            "The older file is chosen for removal")
      (let [new-small-file {:size-bytes (gb->bytes 0.1)
                            :last-access (inst-ms (OffsetDateTime/parse "2070-01-01T00:00:00Z"))}]
        (t/is (= [old-file]
                 (sut/find-old-files [old-file new-file new-small-file]
                                     (gb->bytes 0.1)))
              "File size doesn't change the preference for old files")))))


(t/deftest finding-files-to-remove
  (t/testing "Keep non-expired files below the removal limit"
    (let [delete-at (gb->bytes 9)
          delete-until (gb->bytes 7)
          current-file {:size-bytes (gb->bytes 6)
                        :livemod 10
                        :last-access (inst-ms (OffsetDateTime/parse "2018-02-01T00:00:00Z"))
                        :hash "first-file"}
          expired-file {:size-bytes (gb->bytes 6)
                        :livemod 1
                        :last-access (inst-ms (OffsetDateTime/parse "2018-01-01T00:00:00Z"))
                        :hash "first-file"}]
      (t/is (= {:expired-files [expired-file]}
               (sut/find-files-to-remove* [current-file expired-file]
                                          delete-at
                                          delete-until)))))
  (t/testing "Remove expired and non-expired files to bring the cache down to the lower limit"
    (let [delete-at (gb->bytes 9)
          delete-until (gb->bytes 1)
          current-file {:size-bytes (gb->bytes 6)
                        :livemod 10
                        :last-access (inst-ms (OffsetDateTime/parse "2018-02-01T00:00:00Z"))
                        :hash "first-file"}
          expired-file {:size-bytes (gb->bytes 6)
                        :livemod 1
                        :last-access (inst-ms (OffsetDateTime/parse "2018-01-01T00:00:00Z"))
                        :hash "first-file"}]
      (t/is (= {:expired-files [expired-file]
                :old-files [current-file]}
               (sut/find-files-to-remove* [current-file expired-file]
                                          delete-at
                                          delete-until))))))
