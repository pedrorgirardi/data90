(ns data90.core)

(defn aggregate [formula rows]
  (let [sum-min-max #{:sum :min :max}

        formula-sum-min-max (filter
                              (fn [[_ _ a]]
                                (sum-min-max a))
                              formula)

        formula-reduce (remove
                         (fn [[_ _ a]]
                           (sum-min-max a))
                         formula)

        ag-fn {:sum +
               :min min
               :max max
               :count count}

        ag-reduce (reduce
                    (fn [m [k _ a]]
                      (assoc m k ((or (ag-fn a) a) rows)))
                    {}
                    formula-reduce)

        ag-sum-min-max (reduce
                         (fn [M row]
                           (->> formula-sum-min-max
                                (map
                                  (fn [[k v a]]
                                    (cond
                                      (#{:min :max} a)
                                      (let [v0 (or (get M k) (v row))
                                            v1 (v row)]
                                        [k (some-> v0 ((ag-fn a) (or v1 v0)))])

                                      (= :sum a)
                                      [k (+ (or (get M k) 0) (or (v row) 0))])))
                                (into {})))
                         {}
                         rows)]
    (merge ag-reduce ag-sum-min-max)))

(defn tree
  "A tree grouped, aggregated and sorted.

   D describes how to group and sort, and formula how to aggregate.

   D is a vector of 'branch function', or a vector of
   'branch function' and 'branch comparator' pairs."
  [D formula dataset]
  (let [[d & D-rest] D

        [d-group-by d-comparator] (if (vector? d)
                                    [(first d) (second d)]
                                    [d])

        grouped (group-by d-group-by dataset)

        aggregated (reduce-kv
                     (fn [acc k rows]
                       (let [summary (aggregate formula rows)

                             branches (when (seq D-rest)
                                        (tree D-rest formula rows))]
                         (conj acc [k (if branches
                                        [summary branches]
                                        [summary])])))
                     []
                     grouped)

        sorted (if d-comparator
                 (sort-by first d-comparator aggregated)
                 (sort-by first aggregated))
        sorted (vec sorted)]

    (with-meta sorted {:formula formula})))

(defn tree-summary
  ([tree]
   (let [{:keys [formula]} (meta tree)]
     (tree-summary formula tree)))
  ([formula tree]
   (let [;; Replace source key function with aggregated key.
         formula (map
                   (fn [[k _ a]]
                     [k k a])
                   formula)

         ;; Extract summary of top branches - branch is a vector e.g.: [k [summary branches]].
         rows (map
                (fn [[_ [summary]]]
                  summary)
                tree)]
     (aggregate formula rows))))
