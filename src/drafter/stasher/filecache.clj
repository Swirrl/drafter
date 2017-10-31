(ns drafter.stasher.filecache
  (:require [clojure.core.cache :as cache]))

(def todo-replace-me-with-init-base-cache {}) ;; TODO replace me

(def default-cache-dir "sparql-cache")


(cache/defcache FileCache [cache dir tempdir]
  cache/CacheProtocol

  ;; TODO implement these
  (cache/lookup [_ item]
                )
  (cache/lookup [_ item not-found])
  (cache/has? [_ item]
              false)
  (cache/hit [this item] this)
  (cache/miss [_ item result]
              )
  (cache/evict [_ key]
               )
  (cache/seed [_ base]
              )
  Object
  (toString [_] (str dir))
  )

(defn file-cache-factory
  ([base-cache]
   (file-cache-factory base-cache default-cache-dir))
  ([base-cache dir]
   (fs/mkdir default-cache-dir)
   (FileCache. base-cache (io/file dir) (io/file dir "tmp"))))
