(ns drafter.stasher.filecache
  (:require [clojure.java.io :as io]
            [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log]
            [integrant.core :as ig]
            [me.raynes.fs :as fs]
            [drafter.stasher.cache-key :as ck]
            [cognician.dogstatsd :as dd])
  (:import (java.io File FileOutputStream BufferedOutputStream)
           java.security.MessageDigest
           org.apache.commons.codec.binary.Hex))

(def default-cache-dir "stasher-cache")

(defn- cache-key->hash-key
  "Cache keys at the interface to the cache are maps, but we need to
  repeatably hash these to an MD5 sum for the filename/location on
  disk.  This function takes a cache-key (map) and converts it into an
  MD5 sum."
  [cache-key]
  (let [cache-key-str (pr-str (ck/static-component cache-key))
        md (MessageDigest/getInstance "MD5")]
    (Hex/encodeHexString (.digest md (.getBytes cache-key-str)))))

(defn- cache-key->query-id
  ""
  [cache-key]
  (subs (cache-key->hash-key cache-key) 0 8))

(defn- hash-key->file-path
  "Returns a relative path styled variant of the hash key.
   e.g. given a hash-key of \"0cc23d1cefa62b120c5f3b289503c01e\", convert
  it into the vector [\"0c\" \"c2\" \"0cc23d1cefa62b120c5f3b289503c01e\"]."
  [hash-key]
  (let [sub-dirs (->> hash-key
                      (partition 2)
                      (take 2)
                      (mapv (partial apply str)))]
    (conj sub-dirs hash-key)))

(defn- cache-key->file-name [cache-key fmt]
  (format "%s.%s" (ck/time-component cache-key) (name fmt)))

(defn cache-key->cache-path
  [dir fmt cache-key]
  {:pre [(s/valid? ::ck/cache-key cache-key)]}
  (let [hash (cache-key->hash-key cache-key)
        dirs (hash-key->file-path hash)
        filename (cache-key->file-name cache-key fmt)]
    (apply io/file (concat [dir "cache"]
                           (conj dirs filename)))))

(defn- move-file-to-cache!
  "Move the supplied file into the cache under the specified
  cache-key.  Note this is a mutable operation that moves the file you
  supply on disk."
  [dir ext cache-key temp-file]
  (let [cache-key-fname (cache-key->cache-path dir ext cache-key)]
    (log/debugf "Created cache entry %s for %s query %s"
                cache-key-fname
                (ck/query-type cache-key)
                (cache-key->query-id cache-key))
    (io/make-parents cache-key-fname)
    (fs/rename temp-file cache-key-fname)))

(defn lookup [dir ext item]
  (let [cache-fpath (cache-key->cache-path dir ext item)]
    (when (.exists cache-fpath)
      cache-fpath)))

(defn last-accessed-time [cache-entry-path]
  (fs/mod-time cache-entry-path))

(defn- ^java.io.File create-temp-file!
  "Create and return a temp file inside the cache :dir.  Takes also a
  keyword indicating the file format."
  [cache-dir fmt]
  (let [tmp-dir (io/file cache-dir "tmp")]
    (fs/mkdir tmp-dir)
    (File/createTempFile "stasher" (str "tmp." (name fmt)) tmp-dir)))

(defprotocol StashBackend
  (destination-stream [this cache-key fmt])
  (source-stream [this cache-key fmt]))

(defprotocol DestinationOutputStream
  (cancel-and-close [this]))

(defrecord FileBackend [dir buffer-size]
  StashBackend
  (destination-stream [this cache-key fmt]
    (log/debugf "Creating destination stream for %s query %s"
                (ck/query-type cache-key)
                (cache-key->query-id cache-key))
    (let [temp-file (create-temp-file! dir fmt)]
      (log/tracef "Created temp file %s for %s query %s"
                  temp-file
                  (ck/query-type cache-key)
                  (cache-key->query-id cache-key))
      (proxy [BufferedOutputStream drafter.stasher.filecache.DestinationOutputStream]
          [(FileOutputStream. temp-file) buffer-size]
        (close []
          (proxy-super close)
          (when (fs/exists? temp-file)
            (move-file-to-cache! dir fmt cache-key temp-file)))
        (cancel-and-close []
          (when (fs/exists? temp-file)
            (log/errorf "Deleting temp file without moving into the cache for %s query %s"
                        (ck/query-type cache-key)
                        (cache-key->query-id cache-key))
            (.delete temp-file))
          (.close this)))))
  (source-stream [this cache-key fmt]
    (some-> (lookup dir fmt cache-key) 
            fs/touch
            io/input-stream)))


(defn make-file-backend [opts]
  (let [default-opts {:dir default-cache-dir
                      :buffer-size 8192
                      :persist-on-shutdown? true}
        {:keys [dir] :as opts} (merge default-opts opts)]
    (fs/mkdir dir)
    (fs/mkdir (io/file dir "cache"))
    (fs/mkdir (io/file dir "tmp"))
    (map->FileBackend opts)))



(defmethod ig/init-key :drafter.stasher.filecache/file-backend [_ opts]
  (make-file-backend opts))

(defmethod ig/halt-key! :drafter.stasher.filecache/file-backend [k backend]
  (when-not (:persist-on-shutdown? backend true)
    (log/info "Clearing" k)
    (fs/delete-dir (:dir backend))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Specs


(s/def ::dir fs/directory?)
(s/def ::buffer-size int?)
(s/def ::persist-on-shutdown? boolean?)


(defmethod ig/pre-init-spec :drafter.stasher.filecache/file-backend [_]
  (s/keys :opt-un [::dir ::buffer-size ::persist-on-shutdown?]))


