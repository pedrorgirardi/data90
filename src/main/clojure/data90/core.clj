(ns data90.core)

(defn aggregate [M rows]
  (let [sum-min-max #{:sum :min :max}

        formula-sum-min-max (filter
                              (fn [{:data90/keys [aggregate-with]}]
                                (sum-min-max aggregate-with))
                              M)

        formula-reduce (remove
                         (fn [{:data90/keys [aggregate-with]}]
                           (sum-min-max aggregate-with))
                         M)

        ag-fn {:sum +
               :min min
               :max max
               :count count}

        ag-reduce (reduce
                    (fn [m {aggregate-name :data90/name
                            aggregate-with :data90/aggregate-with}]
                      (assoc m aggregate-name ((or (ag-fn aggregate-with) aggregate-with) rows)))
                    {}
                    formula-reduce)

        ag-sum-min-max (reduce
                         (fn [M row]
                           (->> formula-sum-min-max
                                (map
                                  (fn [{aggregate-name :data90/name
                                        aggregate-by :data90/aggregate-by
                                        aggregate-with :data90/aggregate-with}]
                                    (cond
                                      (#{:min :max} aggregate-with)
                                      (let [v0 (or (get M aggregate-name) (aggregate-by row))
                                            v1 (aggregate-by row)]
                                        [aggregate-name (some-> v0 ((ag-fn aggregate-with) (or v1 v0)))])

                                      (= :sum aggregate-with)
                                      [aggregate-name (+ (or (get M aggregate-name) 0) (or (aggregate-by row) 0))])))
                                (into {})))
                         {}
                         rows)]
    (merge ag-reduce ag-sum-min-max)))

(defn tree
  "A tree grouped, aggregated and sorted.

   D describes how to group and sort, and M how to aggregate."
  [D M dataset]
  (let [[d & D-rest] D

        {d-group-by :data90/group-by
         d-sort-with :data90/sort-with} d

        grouped (group-by d-group-by dataset)

        aggregated (reduce-kv
                     (fn [acc k rows]
                       (let [summary (aggregate M rows)

                             branches (when (seq D-rest)
                                        (tree D-rest M rows))]
                         (conj acc [k (if branches
                                        [summary branches]
                                        [summary])])))
                     []
                     grouped)

        d-sort-asc (fn [x y]
                     (compare x y))

        d-sort-desc (fn [x y]
                      (compare y x))

        d-sort-asc-desc ({:asc d-sort-asc
                          :desc d-sort-desc}
                         d-sort-with)

        d-sort-comparator (or d-sort-asc-desc d-sort-with d-sort-asc)

        sorted (sort-by first d-sort-comparator aggregated)
        sorted (vec sorted)]

    (with-meta sorted {:d d :M M})))

