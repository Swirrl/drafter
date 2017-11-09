(ns drafter.stasher.filecache
  (:require [clojure.core.cache :as cache]
            [me.raynes.fs :as fs]
            [clojure.java.io :as io]
            [clojure.core.cache :as cache]
            [clojure.java.io :as io]
            [grafter.rdf.repository :as repo]
            [grafter.rdf :as rdf]
            [grafter.rdf4j.io :as gio]
            [grafter.rdf.protocols :as pr]
            [me.raynes.fs :as fs]
            [grafter.rdf4j.formats :as fmt]
            [integrant.core :as ig]
            [clojure.spec.alpha :as s]
            [me.raynes.fs :as fs])
  (:import (java.security DigestOutputStream DigestInputStream MessageDigest)
           org.apache.commons.codec.binary.Hex
           org.eclipse.rdf4j.rio.RDFHandler))

(def todo-replace-me-with-init-base-cache {}) ;; TODO replace me

(def default-cache-dir "sparql-cache")

(def default-cache-rdf-format :brf)

(defn backend-rdf-format [cache]
  (or (:backend-rdf-format (.opts cache)) default-cache-rdf-format))

(defn cache-key->hash-key
  "Cache keys at the interface to the cache are maps, but we need to
  repeatably hash these to an MD5 sum for the filename/location on
  disk.  This function takes a cache-key (map) and converts it into an
  MD5 sum."
  [cache-key]
  (let [cache-key-str (pr-str (sort cache-key))
        md (MessageDigest/getInstance "MD5")]
    (Hex/encodeHexString (.digest md (.getBytes cache-key-str)))))

(defn hash-key->file-path
  "Returns a relative path styled variant of the hash key with a
  supplied format extension appended.  e.g. given a hash-key of
  \"0cc23d1cefa62b120c5f3b289503c01e\" and an extension of :ext converts
  it into the string \"0c/c2/0cc23d1cefa62b120c5f3b289503c01e.ext\".
  "
  ([hash-key fmt-extension]
   (let [sub-dirs (->> hash-key
                       (partition 2)
                       (take 2)
                       (mapv (partial apply str)))]
     (apply io/file (conj sub-dirs (str hash-key "." (name fmt-extension)))))))

(defn cache-key->cache-path
  [cache cache-key]
  (let [fmt (backend-rdf-format cache)
        hash-key (hash-key->file-path (cache-key->hash-key cache-key) fmt)]
    (io/file (:dir (.opts cache)) hash-key)))

(defn- move-file-to-cache!
  "Move the supplied file into the cache under the specified
  cache-key.  Note this is a mutable operation that moves the file you
  supply on disk."
  [cache cache-key temp-file]
  (let [cache-key-fname (cache-key->cache-path cache cache-key)]
    (io/make-parents cache-key-fname)
    (fs/rename temp-file cache-key-fname)
    cache))

(defn remove-cache-entry [cache key]
  (let [fname (cache-key->cache-path cache key)]
    (fs/delete fname))
  cache)

(cache/defcache FileCache [base opts]
  cache/CacheProtocol

  ;; TODO implement these
  (cache/lookup [this item]
                (cache/lookup this item nil))
  
  (cache/lookup [this item not-found]
                (let [cache-fpath (cache-key->cache-path this item)]
                  (if (.exists cache-fpath)
                    cache-fpath ;; TODO wrap in rdf-handler
                    not-found)))

  (cache/has? [this item]
              (.exists (cache-key->cache-path this item)))
  
  (cache/hit [this item] this)

  ;; add file to cache here
  (cache/miss [this item result]
              (move-file-to-cache! this item result))
  
  (cache/evict [this key]
               (remove-cache-entry this key))
  
  (cache/seed [this base]

              ;dir tempdir backend-rdf-format
              ;(or (.dir this) dir) (or (.tempdir this) tempdir) (or (.backend-rdf-format this) tempdir)
              (FileCache. base opts))
  
  Object
  (toString [_] (str (:dir opts))))

(s/def ::base-cache #(satisfies? cache/CacheProtocol %))
(s/def ::dir fs/directory?)
(s/def ::persist-on-shutdown? boolean?) ;; use this property when using a cache in unit tests to prevent caching between test runs

;; TODO will need to actually honour the charset support when we
;; support other serialization backends other than :brf
(s/def ::charset string?)               ;; use nil if using :brf format
(s/def ::backend-rdf-format #{:brf :nt :nq :trig :trix :rdf})

(defmethod ig/pre-init-spec :drafter.stasher/filecache [_]
  (s/and (s/keys :opt-un [::base-cache ::dir ::persist-on-shutdown? ::charset ::backend-rdf-format])))

(defn file-cache-factory [opts]
  (let [{:keys [dir base-cache] :as opts} {:base-cache (or (:base-cache opts) {})
                                           :dir (or (:dir opts) default-cache-dir)}]
    (fs/mkdir dir)
    (FileCache. base-cache opts)))

(defmethod ig/init-key :drafter.stasher/filecache [_ opts]
  (file-cache-factory opts))

(defmethod ig/halt-key! :drafter.stasher/filecache [_ cache]
  (when-not (:persist-on-shutdown? (.opts cache) true)
    (fs/delete-dir (:dir (.opts cache)))))

(defn stash-rdf-handler
  ""
  [cache cache-key inner-rdf-handler]
  (let [rdf-format (backend-rdf-format cache)
        temp-file (.createTempFile "stasher" (str "tmp." (name rdf-format)) (io/file (:dir cache) "tmp"))
        make-stream (fmt/select-output-coercer rdf-format)
        stream (make-stream :buffer 8192)
        cache-file-writer (gio/rdf-writer stream :format rdf-format)]

    (reify RDFHandler
      (startRDF [this]
        (.startRDF cache-file-writer)
        (.startRDF inner-rdf-handler))
      (endRDF [this]
        (.endRDF cache-file-writer)
        (.endRDF inner-rdf-handler)
        (.close stream)
        (move-file-to-cache! cache temp-file))
      (handleStatement [this statement]
        (.handleStatement cache-file-writer statement)
        (.handleStatement inner-rdf-handler statement))
      (handleComment [this comment]
        (.handleComment cache-file-writer comment)
        (.handleComment inner-rdf-handler comment))
      (handleNamespace [this prefix-str uri-str]
        (.handleNamespace cache-file-writer prefix-str uri-str)
        (.handleNamespace inner-rdf-handler prefix-str uri-str)))))
