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

(defn within-threshold? [target threshold i]
  (when (<= (* (- 1 threshold) target)
          i
          (* (+ 1 threshold) target))
    i))

(defn rand-long [n]
  (let [max-val 100000000000000]
    (long (rand (if (> n max-val)
                  max-val
                  n)))))

(defn rand-longs [n upto]
  (repeatedly n (comp inc (partial rand-long (dec upto)))))

(defn random-selections
  "Generate n random selections of subsets of items"
  [n items]
  (let [cs (com/count-subsets items)]
    (for [i (rand-longs (min cs
                             n) cs)]
      (com/nth-subset items i))))

(defn- find-close-match
  "Returns the closest match it can find using within-threshold?  A
  match may be one or many items that sum to within-threshold?"
  [target-quota things]
  (->> (for [;;thing things ;;todo tidy names up here
             group things
             ;; partitions (sort-by count (comparator >) (com/partitions group))
                                        ;partitions (shuffle (take 10000 (com/partitions group)))
             item (random-selections 1000 group)
             ;;item partitions
             ;;item (sort-by count (comparator >) (com/subsets group))
             ;item partitions
             :when (within-threshold? target-quota 0.1 (sum item))]
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
   (if (= max-group-size
          gsize)
     []
     (let [total (count items)]
       (cons (partition-all gsize (random-sample prob items))
               (lazy-seq (create-groupings (inc gsize)
                                           (- prob 0.005)
                                           items)))))))

(defn cull
  "Attempt to bring the items within a threshold/margin (currently
  10%) of the target-quota by selecting a set of items to delete.

  Returns the seq of items to remove."
  [target-quota items]
  (first (for [cross-group (shuffle (apply (partial map vector) (create-groupings items)))]
           (find-close-match target-quota cross-group))))

;;;;;;;;;;;;;;;;;;; by stats


(require '[clojurewerkz.statistiker.statistics :as st] )

(defn make-bin-identifiers
  "Takes a list of centiles and partitions them into bins where
  the [lower upper] centiles indicating the extent of the bin.

  e.g. 

  (make-bins [5 10 100])

  ;=> ([0 5] (5 10) (10 100))
  "
  [centile-ranges]
  (drop-last (cons [0 (first centile-ranges)]
                  (partition-all 2 1 centile-ranges))))

(def bin-ids
  "Centile ranges as pairs representing a banding as lower/upper centiles."
  (make-bin-identifiers [5 25
                         45 55   
                         50 75
                         90 95
                         98 99
                         99 99.9
                         99.9 100]))

(defn within-range? [[lower upper :as range] item]
  (< lower item upper))

(defn within-centile-range [[lower-centile upper-centile :as range] items]
  (for [selected (filter (partial within-range? range) items)]

    selected))

(defn bin-boundaries
  "Takes a list of \"centile-ranges\" as an ordered list of
  lower/upper-bounds and returns a map of all lower-upper bound pairs
  to their absolute values as derived from the \"items\" data.
  
  e.g. the call 

  (centile-ranges [5 25 50 75 100] (range 100))

  Will return a map of:

  {(0 5) (0 4.05),
   (5 25) (4.05 24.25),
   (25 50) (24.25 49.5),
   (50 75) (49.5 74.75),
   (75 100) (74.75 99.0)}

  Where the keys are the boundaries expressed as [lower upper]
  centiles and the values are the absolute values (relative to the
  items data) for these ranges."
  [bin-ids items]
  (let [smallest-item (apply min items)
        upper-range (map second bin-ids)
        abs-ranges (let [centiles (cons 0 (st/percentiles items upper-range))]
                     (drop-last (partition-all 2 1 
                                               centiles)))]
    (zipmap bin-ids
            abs-ranges)))

#_(defn build-centile-finder [target-quota range]
  (fn [threshold items]
    (let [abs-range (bin-boundaries range items)]
      (filter #(within-threshold? target-quota threshold %)
              (within-centile-range abs-range items)))))

(defn group-by-bin [bins target-quota all-items]
  (let [find-items (fn [bin items]
                     (filter (partial within-range bin) items))]
    (apply merge (for [[bin-id bin-range] bins
                      :let [items (find-items bin-range all-items)]
                      :when (seq items)]
                   {bin-id items}))))

#_(defn foo [quantiles items]
  (reduce (fn [acc quantile]
            
            ) {} quantile))

(defn eligible-individuals [{target-quota :target-quota
                             threshold :threshold} items]
  (filter (partial within-threshold? target-quota threshold)
          items))

(defn cull-across-bins [opts binned-items]
  (->> binned-items
       vals
       (map (fn [binned-vals] (remove #(< (* (+ 1 (:threshold opts)) (:target-quota opts)) %)
                                     binned-vals)))
       (map (partial split-at 1))))

(defn cull-2
  [{:as opts
    target-quota :target-quota
    threshold :threshold} items]
  (let [distribution (st/fivenum items)
        ;; convert centile groups into absolute data values at boundaries of centiles
        
        individuals (eligible-individuals opts items)]

    
    (if (seq individuals)
      [(apply max individuals)] ;; return biggest eligible item for deletion
      (let [bins (bin-boundaries bin-ids items)
            binned-items (group-by-bin bins target-quota items)]
        ;; else we need to look across multiple items

        
        
        
        bins))
    
    
    
    #_(within-centile-range target-quota threshold (centile-boundaries [45 55]) items)

    #_(let [ind-groups (for [threshold [0.1 0.2]
                             centile (partition 2 1 centile-ranges)
                             :let [items (find-items centile threshold)]
                             :when (seq items)]
                         (find-items centile threshold))
            eligible-individuals (apply concat ind-groups)]
      

        (if (seq eligible-individuals)
          (take 1 eligible-individuals)
          ;; no individual brings us within 10 or 20% of our quota so
          ;; find a "small" grouping that does
          (for [abs-range centile-boundaries ;; largest first
                ])
        
          )))
  )

(comment

  (def test-data [3 2 1 4 2 3 5 6 7 3 4])  ;; total-size 40

  (def test-data (into [] (take 1e6 (repeatedly (partial rand-int 10000)))))


  (def binned (group-by-bin (bin-boundaries bin-ids test-data) 5000 test-data))

  (cull-across-bins {:threshold 0.1 :target-quota 50000} binned)

  )
