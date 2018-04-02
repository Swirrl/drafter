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

(defn diff [a b]
  (Math/abs (- a b)))

(defn sum [items]
  (reduce + items))

#_(defn sorted-by-distance
  "Given a target integer and a seq of items return the items sorted
  by their difference from the target.

  i.e. the first item in the returned sequence will be the item
  closest to the target."
  [target items]
  (let [diffs (map (partial diff target) items)
        items-with-diffs (map vector diffs items)
        sorted (sort-by first items-with-diffs)]
    (map second sorted)))

(def threshold 0.1)

(defn within-threshold? [target i]
  (if (<= (- target (* threshold target))
          i
          (+ target (* threshold target)))
    i))

#_(defn close-enough? [target items]
  (let [threshold 0.1]
    (->> items
         (filter
          #(within-threshold? target %)))))

(defn- find-close-match
  "Returns the closest match it can find using within-threshold?  A
  match may be one or many items that sum to within-threshold?"
  [target-quota things]
  (->> (for [thing things ;;todo tidy names up here
             group thing
             partitions (sort-by count (comparator >) (com/partitions group))
             item partitions
             :when (within-threshold? target-quota (sum item))]
         item)
       first))

(def max-group-size 100)

(defn create-groupings
  "Group items and return an ordered list of things to try.  First we
  try groups of size 1, then 2, 3, 4 up to max-group-size.  This gives
  us opportunities for finding items to cull by culling multiple
  items.

  The probability of an item being in a group gets less as the groups
  grow (This may/may-not be a good idea or)"
  ([items]
   (create-groupings 1 0.8 items))
  ([gsize prob items]
   (println gsize)
   (if (= max-group-size
          gsize)
     []
     (let [total (count items)]
       (cons (partition gsize (random-sample prob items))
               (lazy-seq (create-groupings (inc gsize)
                                           (- prob 0.01)
                                           items)))))))

(defn cull
  "Attempt to bring the items within a threshold/margin (currently
  10%) of the target-quota by selecting a set of items to delete.

  Returns the seq of items to remove."
  [target-quota items]
  (let [groups (create-groupings items)]
    (find-close-match target-quota groups)))




(comment


  (def test-data [3 2 1 4 2 3 5 6 7 3 4])  ;; total-size 40

  (def test-data (take 1e6 (repeatedly (partial rand-int 500))))
  
  (def target-size 30)

  (let [items [6 4 3 3 3 1]]
    (->> items
         (reductions +)
         (map vector items)
         (take-while (fn [[_v acc]] (<= acc 10)))
         (map first)))

  )
