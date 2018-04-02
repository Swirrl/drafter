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

(defn within-threshold? [target i]
  (let [threshold 0.1]
    (if (<= (- target (* threshold target))
            i
            (+ target (* threshold target)))
      i)))

(defn close-enough? [target items]
  (let [threshold 0.1]
    (->> items
         (filter
          #(within-threshold? target %)))))

#_(defn- find-close-match [target-quota items-a items-b items-c]
  (let [i (atom 0)]
    (->> (for [a items-a
               b items-b
               c items-c
               :when (= 3 (count (hash-set a b c)))
               items (com/partitions [a b c])
               item items
               
               :when (within-threshold? target-quota (sum item))
               
               ]
           
           item)
         
         (take 5)
         (sort-by count (comparator <))
         first)))

(defn- find-close-match [target-quota groups]
  (->> (for [group groups
             partitions (sort-by count (comparator >) (com/partitions group))
             item partitions
             :when (within-threshold? target-quota (sum item))]
         item)
       
       first)
  
  #_(reduce (fn [acc [a b c]]
              (if (= 3 (count (hash-set a b c)))
                (concat acc (for [item-group (sort-by count (comparator >) (com/partitions [a b c]))
                                  item item-group
                                  :when (within-threshold? target-quota (sum item))]
                              item))
                acc)) [])
  
  )

(defn cull [target-quota items]
  (let [sample-1 (random-sample 0.1 items)
        sample-2 (random-sample 0.1 items)
        sample-3 (random-sample 0.1 items)
        sample-4 (random-sample 0.1 items)
        sample-5 (random-sample 0.1 items)]
    (find-close-match target-quota (map vector sample-1 sample-2 sample-3 sample-4 sample-5)))
  #_(let [[nearest-item & rest-items] (sorted-by-distance target-quota items)]
     (if nearest-item
       (let [diff-remaining (- target-quota nearest-item)]
         (cond
           #_(= 0 diff-remaining)
           #_[nearest-item]
           (neg? diff-remaining)
           [nearest-item :neg]
           ;; (pos? diff-remaining)
           :else

           (let [sample-1 (random-sample 0.1 items)
                 sample-2 (random-sample 0.1 items)
                 sample-3 (random-sample 0.1 items)]
             (find-close-match target-quota sample-1 sample-2 sample-3))
           
           ))
       
       [])))




(comment


  (def test-data [3 2 1 4 2 3 5 6 7 3 4])  ;; total-size 40

  (def test-data (take 1000000 (repeatedly (partial rand-int 500))))
  
  (def target-size 30)

  (let [items [6 4 3 3 3 1]]
    (->> items
         (reductions +)
         (map vector items)
         (take-while (fn [[_v acc]] (<= acc 10)))
         (map first)))

  )
