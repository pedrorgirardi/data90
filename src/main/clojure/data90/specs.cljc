(ns data90.specs
  (:require
   [clojure.spec.alpha :as s]))

(s/def :data90/group-by ifn?)

(s/def :data90/sort-with (s/or :fn fn? :asc-desc #{:asc :desc}))

(s/def :data90/dimension
  (s/keys
    :req [:data90/group-by]
    :opt [:data90/sort-with]))