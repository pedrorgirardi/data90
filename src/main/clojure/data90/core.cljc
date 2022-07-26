(ns data90.core
  "Data90 is a data transformation library.

  It's unoptimized, but its performance is acceptable for 'small data'.

  It doesn't have any third-party dependency,
  and it supports Clojure and ClojureScript."
  (:require
   [clojure.spec.alpha :as s]

   [data90.specs])
  (:refer-clojure :exclude [flatten]))

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
  from a measure's formula name.

  M is a collection of measure formula.

  A measure is defined by a name,
  an accessor function to read the value from each row,
  and an aggregation function.

  Example:

  (aggregate [[:x' :x :sum]] [{:x 10} {:x 5}])

  ;; => {:x' 15}

  See `data90.core/measure`."
  [M rows]
  (let [;; Coarce measure formula to canonical format.
        M (map measure M)

        ag-fn {:sum +
               :min min
               :max max
               :count count}

        ;; Count and user-defined aggregates.
        custom-aggregates (into [] (remove (comp #{:sum :min :max} :data90/aggregate-with)) M)

        ;; Each aggregation function is called with rows.
        custom-aggregates-result (reduce
                                   (fn [acc {aggregate-name :data90/name
                                             aggregate-with :data90/aggregate-with}]
                                     (let [f (or (ag-fn aggregate-with) aggregate-with)]
                                       (assoc acc aggregate-name (f rows))))
                                   {}
                                   custom-aggregates)

        ;; Sum, min and max aggregates.
        builtin-aggregates (into [] (filter (comp #{:sum :min :max} :data90/aggregate-with)) M)

        builtin-aggregates-result (reduce
                                    (fn [acc row]
                                      (into {}
                                        (map
                                          (fn [{aggregate-name :data90/name
                                                aggregate-by :data90/aggregate-by
                                                aggregate-with :data90/aggregate-with}]
                                            (cond
                                              (#{:min :max} aggregate-with)
                                              (let [v0 (or (get acc aggregate-name) (aggregate-by row))
                                                    v1 (aggregate-by row)]
                                                [aggregate-name (some-> v0 ((ag-fn aggregate-with) (or v1 v0)))])

                                              (= :sum aggregate-with)
                                              [aggregate-name (+ (or (get acc aggregate-name) 0)
                                                                (or (aggregate-by row) 0))])))
                                        builtin-aggregates))
                                    {}
                                    rows)]

    (merge custom-aggregates-result builtin-aggregates-result)))

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
  ([{:keys [D M rows]}]
   (tree D M rows))
  ([D M rows]
   (let [[d & D-rest] D

         M (map measure M)

         {d-group-by :data90/group-by
          d-sort-with :data90/sort-with :as d} (dimension d)

         grouped (group-by d-group-by rows)

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

         sorted (sort-by first comparator aggregated)]

     (with-meta sorted {:d d :M M}))))

(defn- flatten*
  "Returns a flat view, a vector of tuples, of the three."
  [parent tree]
  (reduce
    (fn [rows [v [summary children]]]
      (let [metadata (update (meta parent) :D (fnil conj []) (:d (meta tree)))
            metadata (assoc metadata :M (:M (meta tree)))

            parent' (with-meta (conj parent v) metadata)
            
            row (with-meta (into parent'
                             (map
                               (fn [m]
                                 (summary (:data90/name m))))
                             (:M metadata))
                  metadata)

            rows' (conj rows row)]
        (if (empty? children)
          rows'
          (into rows' (flatten* parent' children)))))
    []
    tree))

(defn flatten
  "Returns a vector of D and M tuples.

  The size of the tuple varies accordingly to D.

  Example:

  Given :D [:operation :operator] and :M [[:H :H :sum]]:

  [[\"Op. A\" 3]
   [\"Op. A\" \"Davi\" 2]
   [\"Op. A\" \"Pedro\" 1]
   [\"Op. B\" 3]
   [\"Op. B\" \"Davi\" 3]
   [\"Op. C\" 3]
   [\"Op. C\" \"Davi\" 3]]

  A row's metadata contains its D and M."
  [tree]
  (flatten* [] tree))

(comment

  (def t
    (tree
      {:D
       [{:data90/group-by :operator
         :data90/sort-with :asc}

        {:data90/group-by :operation
         :data90/sort-with :desc}]

       :M
       [#:data90 {:name :hours
                  :aggregate-by :hours
                  :aggregate-with :sum}]

       :rows
       [{:operation "A" :operator "Pedro" :hours 1}
        {:operation "A" :operator "Davi" :hours 2}
        {:operation "C" :operator "Davi" :hours 2}
        {:operation "D" :operator "Davi" :hours 2}
        {:operation "D" :operator "Davi" :hours 2}
        {:operation "D" :operator "Davi" :hours 2}
        {:operation "D" :operator "Davi" :hours 2}
        {:operation "D" :operator "Davi" :hours 2}
        {:operation "B" :operator "Davi" :hours 1}]}))

  (meta t)
  ;; =>
  {:d #:data90{:name "Operator", :group-by :operator, :sort-with :asc},
   :M (#:data90{:name :hours, :aggregate-by :hours, :aggregate-with :sum})}

  (def rows
    (flatten
      (tree
        {:D [:operation :operator]
         :M [[:H :hours :sum]]
         :rows
         [{:operation "Op. A" :operator "Pedro" :hours 1}
          {:operation "Op. A" :operator "Davi" :hours 2}
          {:operation "Op. B" :operator "Davi" :hours 3}
          {:operation "Op. C" :operator "Davi" :hours 3}]})))

  ;; =>
  [["Op. A" 3]
   ["Op. A" "Davi" 2]
   ["Op. A" "Pedro" 1]
   ["Op. B" 3]
   ["Op. B" "Davi" 3]
   ["Op. C" 3]
   ["Op. C" "Davi" 3]]


  (into []
    (map
      (fn [row]
        [(meta row) row]))
    rows)

  )
