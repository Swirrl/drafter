(ns drafter.import-rdf
  (:require [drafter.queue :as q]))

(def import-queue (q/make-queue 20))

(def backup-queue (q/make-queue 50))

(def errors (atom []))

(defn record-error [buf-size]
  (fn [ex]
    (let [apply-error (fn [prev-errors]
                        (->> (conj prev-errors ex)
                             (drop (- (inc (count prev-errors)) buf-size))
                             (apply vector)))]
      (swap! errors apply-error))))

(defn import-rdf-file [{:keys [id file endpoint-destination] :as msg}]
  (println (str "Import job " id " from file: " file " to endpoint: " endpoint-destination))
  (when-not (q/offer! backup-queue msg)
    (throw (ex-info "There are too many backups in progress." {:type :too-many-backups}))))

(defn upload-to-s3 [{:keys [id file]}]
  (println (str "Uploading file: " file " to S3")))

(comment

  (def import-queue-manager (q/process-queue import-queue import-rdf-file (record-error 20)))

  (def backup-queue-manager (q/process-queue backup-queue upload-to-s3 (record-error 20)))

  )
