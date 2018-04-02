(ns drafter.stasher.cache-clearer
  (:require [integrant.core :as ig]
            [me.raynes.fs :as fs]
            [clojure.math.combinatorics :as com]))

(defn file-sizes
  "Returns all files recursively within dir and sorts them into
  descending order by file size."
  [dir]
  (let [descending (comparator >)]
    (->> dir
         fs/file 
         (tree-seq fs/directory? fs/list-dir)
         (filter fs/file?)
         (sort-by fs/size descending))))

(defmethod ig/init-key :drafter.stasher/cache-clearer [_ opts]
  )

#_(defn smallest-batch [target-quota items]
  (let [items (sort (comparator >) items)]
    (:items (reduce (fn [{:keys [sum items] :as acc} i]
                     (cond
                       (>= sum target-quota) (reduced acc)
                       :else (-> acc
                                 (update :items #(conj % i))
                                 (update :sum + i))))
                    {:sum 0 :items []} items))))

(defn diff [a b]
  (Math/abs (- a b)))

(defn sum [items]
  (reduce + items))



(defn sorted-by-distance
  "Given a target integer and a seq of items return the items sorted
  by their difference from the target.

  i.e. the first item in the returned sequence will be the item
  closest to the target."
  [target items]
  (let [diffs (map (partial diff target) items)
        items-with-diffs (map vector diffs items)
        sorted (sort-by first items-with-diffs)]
    (map second sorted)))

(defn cull [target-quota items]
  (let [[nearest-item & rest-items] (sorted-by-distance target-quota items)]
    (if nearest-item
      (let [diff-remaining (- target-quota nearest-item)]
        (cond
          (= 0 diff-remaining)
          [nearest-item]
          (neg? diff-remaining)
          [nearest-item]
          :else
          (cons nearest-item (cull diff-remaining
                                   rest-items))))
      [])))


#_(defn select-items-to-cull
  "Sum weight of items, if the total is more than the allowed quota
  select a random sample of the items at a probability P "
  ([quota items] (select-items-to-cull quota items 0.1))
  ([quota items probability]
   (println probability)
   (let [total-size (reduce + items)
         diff (Math/abs (- quota total-size))]
     (if (> total-size quota)
       (let [sample (random-sample probability items)
             quantity (reduce + sample)
             sample-big-enough-for-cull? (>= quantity diff)]
         (if sample-big-enough-for-cull?
           (let [batch (smallest-batch diff sample)
                 batch-size (reduce + batch)
                 batch-big-enough-for-cull? (>= batch-size diff)]
             (if batch-big-enough-for-cull?
               batch
               (select-items-to-cull quota items (+ 0.1 probability)) ;; try again with a bigger sample probability
               ))
           (select-items-to-cull quota items (+ 0.1 probability))))))))



(comment


  (def test-data [3 2 1 4 2 3 5 6 7 3 4])  ;; total-size 40
  
  (def target-size 30)

  (let [items [6 4 3 3 3 1]]
    (->> items
         (reductions +)
         (map vector items)
         (take-while (fn [[_v acc]] (<= acc 10)))
         (map first)))

  )
