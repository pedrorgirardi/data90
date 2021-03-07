(ns data90.core)

(defn aggregate [formula dataset]
  (reduce
    (fn [M row]
      (->> formula
           (map
             (fn [[k v a]]
               [k (case a
                    :sum
                    (+ (or (get M k) 0)
                       (or (v row) 0))

                    nil)]))
           (into {})))
    {}
    dataset))

(defn tree-group [D formula dataset]
  (let [[d & D-rest] D]
    (with-meta
      (->> (group-by d dataset)
           (reduce-kv
             (fn [m k k-dataset]
               (let [summary (aggregate formula k-dataset)

                     children (when (seq D-rest)
                                (tree-group D-rest formula k-dataset))]
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

        dataset (map first (vals tree-grouped))]
    (aggregate formula dataset)))
