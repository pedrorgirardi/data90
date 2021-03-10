(ns data90.core)

(defn aggregate [formula rows]
  (reduce
    (fn [M row]
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

            ag-sum-min-max (->> formula-sum-min-max
                                (map
                                  (fn [[k v a]]
                                    (cond
                                      (#{:min :max} a)
                                      (let [v0 (or (get M k) (v row))
                                            v1 (v row)]
                                        [k (some-> v0 ((ag-fn a) (or v1 v0)))])

                                      (= :sum a)
                                      [k (+ (or (get M k) 0) (or (v row) 0))])))
                                (into {}))

            ag-reduce (reduce
                        (fn [m [k _ a]]
                          (assoc m k ((or (ag-fn a) a) rows)))
                        {}
                        formula-reduce)]

        (merge ag-sum-min-max ag-reduce)))
    {}
    rows))

(defn tree-group [D formula dataset]
  (let [[d & D-rest] D]
    (with-meta
      (->> (group-by d dataset)
           (reduce-kv
             (fn [m k rows]
               (let [summary (aggregate formula rows)

                     children (when (seq D-rest)
                                (tree-group D-rest formula rows))]
                 (assoc m k (if children
                              [summary children]
                              [summary]))))
             {}))
      {:formula formula})))

(defn summary [tree-grouped]
  (let [{:keys [formula]} (meta tree-grouped)

        ;; Adjust formula since tree-grouped
        ;; already had its keys mapped.
        formula (map
                  (fn [[k _ a]]
                    [k k a])
                  formula)

        rows (map first (vals tree-grouped))]
    (aggregate formula rows)))
