(ns user
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.pprint :as pprint]
            [clojure.tools.namespace.repl :refer [refresh]]

            [data90.core :as data90]))

(comment

  (def dataset
    [{:operation_code "P1"
      :operation_name "Pulverização"
      :operator_code "O1"
      :operator_name "Paulinho"
      :hours 3}

     {:operation_code "P1"
      :operation_name "Pulverização"
      :operator_code "O2"
      :operator_name "Carlinhos"
      :hours 7}

     {:operation_code "A1"
      :operation_name "Adubação"
      :operator_code "O2"
      :operator_name "Carlinhos"
      :hours 15}])


  (= {:total-x 15, :total-y 15}
     (data90/aggregate
       [[:total-x :x :sum]
        [:total-y :y :sum]]
       [{:x 10 :y 5}
        {:x 5 :y 10}]))

  (data90/tree
    [:operator_name]
    [[:hours-sum :hours :sum]]
    dataset)

  (defn tree-ungroup
    ([D tree]
     (tree-ungroup D tree {}))
    ([D tree ancestors]
     (let [[d & D-rest] D

           [d-key _] (if (vector? d)
                       [(first d)]
                       [d])]
       (reduce
         (fn [rows [d-val [summary branches]]]
           (let [leaf? (empty? branches)

                 ancestors' (merge ancestors {d-key d-val})

                 rows' (conj rows (merge {:d d-key
                                          :is_leaf leaf?}
                                         ancestors'
                                         summary))]
             (if leaf?
               rows'
               (into rows' (tree-ungroup D-rest branches ancestors')))))
         []
         tree))))

  (let [D [:operator_name :operation_name]]
    (->> dataset
         (data90/tree D [[:hours-sum :hours :sum]])
         (tree-ungroup D)))

  )