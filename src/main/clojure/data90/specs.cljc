(ns data90.specs
  (:require
   [clojure.spec.alpha :as s]))

(s/def :data90/name (s/or :string string? :keyword keyword?))

(s/def :data90/group-by ifn?)

(s/def :data90/sort-with (s/or :fn fn? :asc-desc #{:asc :desc}))

(s/def :data90/dimension
  (s/keys
    :req [:data90/group-by]
    :opt [:data90/sort-with]))

(s/def :data90/aggregate-by ifn?)

(s/def :data90/aggregate-with
  (s/or
    :keyword #{:sum
               :min
               :max
               :count}

    :function fn?))

(s/def :data90/measure
  (s/keys
    :req [:data90/name
          :data90/aggregate-by
          :data90/aggregate-with]))