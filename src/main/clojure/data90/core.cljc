(ns data90.core
  (:require
   [clojure.spec.alpha :as s]

   [data90.specs]))

(defn dimension [x]
  (cond
    (map? x)
    x

    (vector? x)
    (merge {:data90/group-by (first x)}
           (when-let [sort-with (second x)]
             {:data90/sort-with sort-with}))

    (ifn? x)
    {:data90/group-by x}

    :else
    (throw (ex-info (str "Can't create dimension from " (pr-str x) ".") {:x x}))))

(s/fdef dimension
  :args (s/cat :x any?)
  :ret :data90/dimension)

(defn measure [x]
  (cond
    (map? x)
    x

    (vector? x)
    (let [[ag-name ag-by ag-with] x]
      {:data90/name ag-name
       :data90/aggregate-by ag-by
       :data90/aggregate-with ag-with})

    :else
    (throw (ex-info (str "Can't create measure from " (pr-str x) ".") {:x x}))))

(s/fdef measure
  :args (s/cat :x any?)
  :ret :data90/measure)

(defn aggregate
  "Aggregate rows.

  Returns a map where keys are derived
  from a measure's specification name.

  M is a collection of measure specification.

  A measure is defined by a name, an aggregation function,
  and an accessor function to read the value from each row.

  Example:

  (aggregate [[:x-sum :x :sum]] [{:x 10} {:x 5}])

  ;; => {:x-sum 15}

  See `data90.core/measure`."
  [M rows]
  (let [sum-min-max #{:sum :min :max}

        M (map measure M)

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

   D describes how to group and sort, and M how to aggregate.

   There are a few possible ways to describe a dimension: as a map, vector or function.

   It's most convenient to describe it as a function e.g: :a, and it's probably
   what you need most of the time.

   In case you also need to specify a sort comparator,
   you can use a vector form instead: [:a >].

   The map form is the most verbose, but it's the canonical
   representation of a dimension."
  [D M dataset]
  (let [[d & D-rest] D

        M (map measure M)

        {d-group-by :data90/group-by
         d-sort-with :data90/sort-with :as d} (dimension d)

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

        sort-asc (fn [x y]
                   (compare x y))

        sort-desc (fn [x y]
                    (compare y x))

        sort-asc-desc ({:asc sort-asc
                        :desc sort-desc}
                       d-sort-with)

        comparator (or sort-asc-desc d-sort-with sort-asc)

        sorted (sort-by first comparator aggregated)
        sorted (vec sorted)]

    (with-meta sorted {:d d :M M})))

