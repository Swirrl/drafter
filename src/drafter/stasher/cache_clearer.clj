(ns drafter.stasher.cache-clearer
  (:require [integrant.core :as ig]
            [me.raynes.fs :as fs]
            [clojure.tools.logging :as log]
            [clojure.java.io :as io]
            [clojure.string :as str]))

(defn ->entry-meta-data [file]
  (let [file (fs/file file)
        filename (fs/name file)
        [livemod draftmod] (str/split filename #"-")
        parent (fs/parent file)
        hash (fs/name parent)
        ext (fs/extension file)
        size-bytes (fs/size file)
        last-access (fs/mod-time file)]
    (try
      {:file file
       :hash hash
       :livemod (when-not (= livemod "empty") (Long/parseLong livemod))
       :draftmod (some->> draftmod Long/parseLong)
       :size-bytes size-bytes
       :last-access last-access}
      (catch Exception e
        {:exception e
         :file file}))))

(defn- valid? [entry-meta-data]
  (or (not (contains? entry-meta-data :exception))
      (log/warnf "invalid file in cache %s" (:file entry-meta-data))))


(defn all-files
  [dir]
  (->> dir
       fs/file
       (tree-seq fs/directory? fs/list-dir)
       (filter fs/file?)
       (map ->entry-meta-data)
       (filter valid?)))

(defn find-biggest [key-fn coll]
  (last (sort (map key-fn coll))))

(defn find-expired-for-hash
  "Given a list of files with the same hash, return those that are expired"
  [files]
  {:pre [(apply = (map :hash files))]}
  (let [latest-livemod (find-biggest :livemod files)
        drafts-on-latest-live (filter #(and (= latest-livemod (:livemod %))
                                            (contains? % :draftmod))
                                      files)
        latest-draft (find-biggest :draftmod drafts-on-latest-live)]
    (remove
     (fn [{:keys [livemod draftmod]}]
       (and (= latest-livemod livemod)
            (or (nil? draftmod)
                (= latest-draft draftmod))))
     files)))

(defn find-expired
  "Given a list of files, return those that are expired"
  [files]
  (let [grouped-by-hash (group-by :hash files)]
    (mapcat find-expired-for-hash (vals grouped-by-hash))))

(defn measure-size [files]
  (reduce + 0 (map :size files)))

(defn ->bytes [gb-size]
  (* gb-size 1024 1024 1024))

(defn find-old-files [files bytes-to-remove]
  (:files (reduce (fn [{:keys [bytes-to-remove files] :as acc} next-val]
                    (if (<= bytes-to-remove 0)
                      (reduced acc)
                      (let [next-size (:size next-val)]
                        {:bytes-to-remove (- bytes-to-remove next-size)
                         :files (conj files next-val)})))
                  {:bytes-to-remove bytes-to-remove
                   :files []}
                  (sort-by :last-access files))))

(defn find-files-to-remove*
  [all-files delete-at delete-until]
  {:pre [(< 0 delete-until delete-at)]}
  (let [cache-size (measure-size all-files)]
    (when (> cache-size delete-at)
      (let [expired-files (find-expired all-files)
            expired-size (measure-size expired-files)]
        (log/debugf "Found %d expired files measuring %.2fGB to delete"
                    (count expired-files)
                    (/ expired-size 1024 1024 1024.0))
        (merge
          {:expired-files expired-files}
          (when (<= delete-until (- cache-size expired-size))
            ;; Find more if we haven't reached out limit
            (let [not-expired-files (remove (set expired-files) all-files)
                  files-to-remove (find-old-files not-expired-files
                                                  (- cache-size delete-until expired-size))]
              {:old-files files-to-remove})))))))

(defn find-files-to-remove [all-files max-cache-size-gb delete-at delete-until]
  {:pre [(<= 0 delete-until delete-at 1)]}
  (let [delete-at (int (* (->bytes max-cache-size-gb) delete-at))
        delete-until (int (* (->bytes max-cache-size-gb) delete-until))]
    (find-files-to-remove* all-files delete-at delete-until)))



(defn start! [opts]
  {})

(defn stop! [clearer])

(defmethod ig/init-key :drafter.stasher/cache-clearer [_ opts]
  (start! opts))

(defmethod ig/halt-key! :drafter.stasher/cache-clearer [_ cache-clearer]
  (stop! cache-clearer))



