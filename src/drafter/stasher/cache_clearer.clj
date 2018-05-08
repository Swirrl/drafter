(ns drafter.stasher.cache-clearer
  (:require [integrant.core :as ig]
            [me.raynes.fs :as fs]
            [clojure.tools.logging :as log]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.spec.alpha :as s])
  (:import [java.util.concurrent Executors ScheduledExecutorService TimeUnit]))

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
  (reduce + 0 (map :size-bytes files)))

(defn ->bytes [gb-size]
  (* gb-size 1024 1024 1024))

(defn find-old-files [files bytes-to-remove]
  (:files (reduce (fn [{:keys [bytes-to-remove files] :as acc} next-val]
                    (if (<= bytes-to-remove 0)
                      (reduced acc)
                      (let [next-size (:size-bytes next-val)]
                        {:bytes-to-remove (- bytes-to-remove next-size)
                         :files (conj files next-val)})))
                  {:bytes-to-remove bytes-to-remove
                   :files []}
                  (sort-by :last-access files))))

(defn find-files-to-remove*
  [all-files archive-at archive-until]
  {:pre [(<= 0 archive-until archive-at)]}
  (let [cache-size (measure-size all-files)]
    (when (> cache-size archive-at)
      (let [expired-files (find-expired all-files)
            expired-size (measure-size expired-files)]
        (log/debugf "Found %d expired files for removal measuring %.2fGB"
                    (count expired-files)
                    (/ expired-size 1024 1024 1024.0))
        (merge
         {:expired-files expired-files}
         (when (<= archive-until (- cache-size expired-size))
           ;; Find more if we haven't reached out limit
           (let [not-expired-files (remove (set expired-files) all-files)
                 old-files (find-old-files not-expired-files
                                           (- cache-size archive-until expired-size))
                 old-files-size (measure-size old-files)]
             (log/debugf "Found %d old files for removal measuring %.2fGB"
                         (count old-files)
                         (/ old-files-size 1024 1024 1024.0))
             {:old-files old-files})))))))

(defn find-files-to-remove [all-files max-cache-size-gb archive-at archive-until]
  {:pre [(<= 0 archive-until archive-at 1)]}
  (let [archive-at (long (* (->bytes max-cache-size-gb) archive-at))
        archive-until (long (* (->bytes max-cache-size-gb) archive-until))]
    (find-files-to-remove* all-files archive-at archive-until)))

(defn archive! [archive-file! cache-dir max-cache-size-gb archive-at archive-until]
  (let [files (all-files (fs/file cache-dir "cache"))
        {:keys [expired-files
                old-files]} (find-files-to-remove files
                                                  max-cache-size-gb
                                                  archive-at
                                                  archive-until)]
    (log/infof "Archiving %d expired files and %d old files"
               (count expired-files)
               (count old-files))
    (dorun (map archive-file! expired-files))
    (dorun (map archive-file! old-files))))

(defn archive-file [cache-dir archive-dir {:keys [file]}]
  (let [filename (.getName file)
        new-file (fs/file cache-dir archive-dir filename)]
    (fs/rename file new-file)
    ;; Touch to set the "moved to archive" time
    (fs/touch new-file)))

(defn- delete-files [files]
  (log/debugf "Deleting %d files from archive" (count files))
  (doseq [file files]
    (log/tracef "Deleting file %s" file)
    (fs/delete file)))

(defn clean-archives! [cache-dir archive-dir archive-ttl]
  (let [delete-cutoff (- (System/currentTimeMillis)
                         (.toMillis (TimeUnit/MINUTES) archive-ttl))]
    (log/debugf "Cleaning archive, all files dated before %s will be removed"
                (java.util.Date. delete-cutoff))
    (->> (fs/file cache-dir archive-dir)
         fs/list-dir
         (filter (comp (partial > delete-cutoff) fs/mod-time))
         delete-files)))

(defn schedule [scheduler f delay period]
  (.scheduleAtFixedRate scheduler f delay period TimeUnit/MINUTES))

(defn stop-scheduled-task [task]
  (.cancel task false))

(defn start! [opts]
  (let [defaults {:archive-at 0.8
                  :archive-until 0.6
                  :delay 10
                  :period 30
                  :archive-dir "archive"
                  :archive-ttl 240}
        {:keys [scheduler
                cache-dir
                max-cache-size-gb
                archive-at
                archive-until
                archive-dir
                archive-ttl
                delay
                period]} (merge defaults opts)
        archive-fn (partial archive! (partial archive-file cache-dir archive-dir)
                            cache-dir max-cache-size-gb archive-at archive-until)
        clean-fn (partial clean-archives! cache-dir archive-dir archive-ttl)]
    (assert (<= 0 archive-until archive-at 1))
    (log/infof "CacheCleaner process will run in %d minutes and then every %d minutes after. The cache size is configured to %f and cleanup will occur at %f of capacity and reduce it to %f."
               delay period max-cache-size-gb archive-at archive-until)
    (fs/mkdir (io/file cache-dir archive-dir))
    {:archiver (schedule scheduler archive-fn delay period)
     :archiver-fn archive-fn
     :cleaner (schedule scheduler clean-fn delay period)
     :cleaner-fn clean-fn}))

(defn stop! [{:keys [archiver cleaner]}]
  (stop-scheduled-task archiver)
  (stop-scheduled-task cleaner))

(defmethod ig/pre-init-spec :drafter.stasher/cache-clearer [_]
  (s/keys :req-un [::scheduler ::cache-dir ::max-cache-size-gb]
          :opt-un [::archive-at ::archive-until ::archive-dir ::archive-ttl
                   ::delay ::period]))

(defmethod ig/init-key :drafter.stasher/cache-clearer [_ opts]
  (start! opts))

(defmethod ig/halt-key! :drafter.stasher/cache-clearer [_ cache-clearer]
  (stop! cache-clearer))

(defmethod ig/init-key :drafter.stasher.cache-clearer/scheduler [_ opts]
  (let [{:keys [pool-size]} opts]
    (Executors/newScheduledThreadPool pool-size)))

(defmethod ig/halt-key! :drafter.stasher.cache-clearer/scheduler [_ scheduler]
  (.shutdown scheduler))
