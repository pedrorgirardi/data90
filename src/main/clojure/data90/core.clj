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
                       ;; I'm not sure if it's a good idea to fallback to looking up 'k'.
                       (or (v row) (k row) 0))

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
        dataset (map first (vals tree-grouped))]
    (aggregate formula dataset)))
