(ns data90.core-test
  (:require [clojure.test :refer :all]
            [clojure.java.io :as io]

            [data90.core :as data90])
  (:import (java.time LocalDate)))

(def dataset1 (read-string (slurp (io/resource "dataset1.edn"))))

(deftest aggregate-test
  (testing "Sum"
    (is (= {}
           (data90/aggregate
             nil
             nil)))

    (is (= {}
           (data90/aggregate
             nil
             [])))

    (is (= {}
           (data90/aggregate
             nil
             [{:x 3 :y 2}])))

    (is (= {:sum-x 3
            :sum-y 2}
           (data90/aggregate
             [[:sum-x :x :sum]
              [:sum-y :y :sum]]
             [{:x 3 :y 2}])))

    (is (= {}
           (data90/aggregate
             nil
             [{:x 3 :y 2}
              {:x 2 :y 3}])))

    (let [rows [{:x 3 :y 2}
                {:x 2 :y 3}]]
      (is (= {:sum-x 5 :sum-y 5}
             (data90/aggregate
               [[:sum-x :x :sum]
                [:sum-y :y :sum]]
               rows))))

    (let [rows [{:hours 3}
                {:hours 7}]]
      (is (= {:hours 10}
             (data90/aggregate
               [[:hours :hours :sum]]
               rows))))

    (let [rows [{:hours 3}
                {:hours nil}]]
      (is (= {:total 3}
             (data90/aggregate
               [[:total :hours :sum]]
               rows))))

    (let [rows [{:hours nil}
                {:hours nil}]]
      (is (= {:total 0}
             (data90/aggregate
               [[:total :hours :sum]]
               rows)))))

  (testing "Min"
    (is (= {:min-x -1}
           (data90/aggregate
             [[:min-x :x :min]]
             [{:x 1}
              {:x nil}
              {:x -1}]))))

  (testing "Max"
    (is (= {:max-x 3}
           (data90/aggregate
             [[:max-x :x :max]]
             [{:x 1}
              {:x 2}
              {:x nil}
              {:x 3}
              {:x -10}]))))

  (testing "Count"
    (is (= {:count 3}
           (data90/aggregate
             [[:count :x :count]]
             [{}
              {}
              {}]))))

  (testing "User defined"
    (is (= {:count 3}
           (data90/aggregate
             [[:count :x (fn [rows]
                           (count rows))]]
             [{}
              {}
              {}])))

    (is (= {:min 1}
           (data90/aggregate
             [[:min :x (fn [rows]
                         (->> rows
                              (map :x)
                              (reduce min)))]]
             [{:x 1}
              {:x 2}
              {:x 3}]))))

  (testing "All combined"
    (is (= {:count 3
            :max 3
            :min 1
            :sum 6
            :user-count 3}
           (data90/aggregate
             [[:sum :x :sum]
              [:min :x :min]
              [:max :x :max]
              [:count :x :count]
              [:user-count :x count]]
             [{:x 1}
              {:x 2}
              {:x 3}])))))

(deftest tree-test
  (let [dataset [{:operation_code "P1"
                  :operation_name "Pulverização"
                  :operator_code "O1"
                  :operator_name "Paulinho"
                  :date "2021-03-05"
                  :hours 3}

                 {:operation_code "P1"
                  :operation_name "Pulverização"
                  :operator_code "O2"
                  :operator_name "Carlinhos"
                  :date "2021-03-05"
                  :hours 7}

                 {:operation_code "A1"
                  :operation_name "Adubação"
                  :operator_code "O2"
                  :operator_name "Carlinhos"
                  :date "2021-03-06"
                  :hours 15}]]

    (testing "Sort"
      (is (= [[1
               [{:sum 1}]]
              [2
               [{:sum 1}]]
              [3
               [{:sum 1}]]]

             (data90/tree
               [#:data90 {:group-by :a}]
               [[:sum :x :sum]]
               [{:a 1 :x 1}
                {:a 2 :x 1}
                {:a 3 :x 1}])))

      (is (= [[3
               [{:sum 1}]]
              [2
               [{:sum 1}]]
              [1
               [{:sum 1}]]]

             (data90/tree
               [#:data90 {:group-by :a
                          :sort-by
                          (fn [_]
                            (fn [x1 x2]
                              (compare x2 x1)))}]
               [[:sum :x :sum]]
               [{:a 1 :x 1}
                {:a 2 :x 1}
                {:a 3 :x 1}])))

      (is (= [["2021-01-01"
               [{:sum 1}]]
              ["2021-01-02"
               [{:sum 1}]]
              ["2021-01-03"
               [{:sum 1}]]]

             (data90/tree
               [#:data90 {:name :a
                          :group-by :a
                          :sort-by
                          (fn [_]
                            (fn [x1 x2]
                              (compare (LocalDate/parse x1) (LocalDate/parse x2))))}]
               [[:sum :x :sum]]
               [{:a "2021-01-01" :x 1}
                {:a "2021-01-02" :x 1}
                {:a "2021-01-03" :x 1}])))

      (is (= [["2021-01-03"
               [{:sum 1}]]
              ["2021-01-02"
               [{:sum 1}]]
              ["2021-01-01"
               [{:sum 1}]]]

             (data90/tree
               [#:data90 {:name "a"
                          :group-by :a
                          :sort-by
                          (fn [_]
                            (fn [x1 x2]
                              (compare (LocalDate/parse x2) (LocalDate/parse x1))))}]
               [[:sum :x :sum]]
               [{:a "2021-01-01" :x 1}
                {:a "2021-01-02" :x 1}
                {:a "2021-01-03" :x 1}])))

      (is (= [[1
               [{:sum 3}
                [["C"
                  [{:sum 1}]]
                 ["B"
                  [{:sum 1}]]
                 ["A"
                  [{:sum 1}]]]]]
              [2
               [{:sum 2}
                [["B"
                  [{:sum 1}]]
                 ["A"
                  [{:sum 1}]]]]]]

             (data90/tree
               ;; :a is asc; :b is desc.
               [#:data90 {:group-by :a
                          :sort-by
                          (fn [_]
                            (fn [x1 x2]
                              (compare x1 x2)))}

                #:data90 {:group-by :b
                          :sort-by
                          (fn [_]
                            (fn [x1 x2]
                              (compare x2 x1)))}]
               [[:sum :x :sum]]
               [{:a 1 :b "A" :x 1}
                {:a 1 :b "B" :x 1}
                {:a 1 :b "C" :x 1}
                {:a 2 :b "A" :x 1}
                {:a 2 :b "B" :x 1}]))))

    (is (= [["Adubação"
             [{:sum 15}]]

            ["Pulverização"
             [{:sum 10}]]]

           (data90/tree
             [#:data90 {:group-by :operation_name}]
             [[:sum :hours :sum]]
             dataset)))

    (is (= [["Carlinhos"
             [{:sum 22}]]

            ["Paulinho"
             [{:sum 3}]]]

           (data90/tree
             [#:data90 {:group-by :operator_name}]
             [[:sum :hours :sum]]
             dataset)))

    (is (= [[["O1" "Paulinho"]
             [{:sum 3}]]
            [["O2" "Carlinhos"]
             [{:sum 22}]]]

           (data90/tree
             [#:data90 {:group-by (juxt :operator_code :operator_name)}]
             [[:sum :hours :sum]]
             dataset)))

    (is (= [["Carlinhos"
             [{:hours 22}
              [["Adubação"
                [{:hours 15}]]
               ["Pulverização"
                [{:hours 7}]]]]]
            ["Paulinho"
             [{:hours 3}
              [["Pulverização"
                [{:hours 3}]]]]]]

           (data90/tree
             [#:data90 {:group-by :operator_name}
              #:data90 {:group-by :operation_name}]
             [[:hours :hours :sum]]
             dataset)))

    (is (= [["Carlinhos"
             [{:hours 22}
              [["2021-03-05"
                [{:hours 7}
                 [["Pulverização"
                   [{:hours 7}]]]]]
               ["2021-03-06"
                [{:hours 15}
                 [["Adubação"
                   [{:hours 15}]]]]]]]]
            ["Paulinho"
             [{:hours 3}
              [["2021-03-05"
                [{:hours 3}
                 [["Pulverização"
                   [{:hours 3}]]]]]]]]]

           (data90/tree
             [#:data90 {:group-by :operator_name}
              #:data90 {:group-by :date}
              #:data90 {:group-by :operation_name}]
             [[:hours :hours :sum]]
             dataset)))

    (testing "Nil dataset"
      (is (= [] (data90/tree
                  [#:data90 {:group-by :x}]
                  [[:x :x :sum]]
                  nil)))
      (is (= [] (data90/tree nil nil nil))))

    (testing "Empty dataset"
      (is (= [] (data90/tree
                  [#:data90 {:group-by :x}]
                  [[:x :x :sum]]
                  []))))

    (testing "Metadata"
      (is (= {:formula [[:x :x :sum]]} (meta (data90/tree
                                               [#:data90 {:group-by :x}]
                                               [[:x :x :sum]]
                                               [])))))

    (testing "Dataset 1"
      (is (= [["Chapadão do Sul"
               [{:hours 71.99999999999999}
                [["Diurno"
                  [{:hours 71.99999999999999}
                   [["Alecio Morais de Almeida"
                     [{:hours 12.0}
                      [["20210305"
                        [{:hours 12.0}
                         [["Auto Deslocamento"
                           [{:hours 0.04909527777777778}]]
                          ["Clima"
                           [{:hours 11.548865833333334}]]
                          ["Fim de Turno"
                           [{:hours 0.19172638888888888}]]
                          ["Motor Ocioso Sem Apontamento"
                           [{:hours 0.20390472222222222}]]
                          ["Pulverização"
                           [{:hours 0.006407777777777778}]]]]]]]]
                    ["Danilo Eduardo Maciel Botelho"
                     [{:hours 12.0}
                      [["20210305"
                        [{:hours 12.0}
                         [["Abastecimento de Calda"
                           [{:hours 0.40753666666666666}]]
                          ["Abastecimento de Diesel"
                           [{:hours 0.12691833333333333}]]
                          ["Aguardando Ordem"
                           [{:hours 0.3666872222222222}]]
                          ["Aplicação de Limpeza"
                           [{:hours 1.4583391666666667}]]
                          ["Auto Deslocamento"
                           [{:hours 1.5929277777777777}]]
                          ["Clima"
                           [{:hours 0.9764066666666666}]]
                          ["Desligamento Sem Apontamento"
                           [{:hours 0.460545}]]
                          ["Fim de Turno"
                           [{:hours 1.9992377777777777}]]
                          ["Limpeza de Bico e Filtro"
                           [{:hours 0.36504888888888887}]]
                          ["Motor Ocioso Sem Apontamento"
                           [{:hours 1.3392155555555556}]]
                          ["Parada Sem Apontamento"
                           [{:hours 1.8406713888888888}]]
                          ["Pulverização"
                           [{:hours 0.7775436111111111}]]
                          ["Refeição"
                           [{:hours 0.28892194444444447}]]]]]]]]
                    ["Leandro Caires Santos"
                     [{:hours 12.000000000000002}
                      [["20210305"
                        [{:hours 12.000000000000002}
                         [["Abastecimento de Calda"
                           [{:hours 0.41191555555555553}]]
                          ["Aguardando Ordem"
                           [{:hours 4.624975}]]
                          ["Auto Deslocamento"
                           [{:hours 0.5903263888888889}]]
                          ["Clima"
                           [{:hours 3.983371388888889}]]
                          ["Fim de Turno"
                           [{:hours 1.6461369444444445}]]
                          ["Pulverização"
                           [{:hours 0.7432747222222222}]]]]]]]]
                    ["Nabor Botós Loureiro de Moraes"
                     [{:hours 12.0}
                      [["20210305"
                        [{:hours 12.0}
                         [["Abastecimento de Calda"
                           [{:hours 0.0018997222222222221}]]
                          ["Auto Deslocamento"
                           [{:hours 0.9672452777777778}]]
                          ["Clima"
                           [{:hours 5.184098611111111}]]
                          ["Desligamento Sem Apontamento"
                           [{:hours 0.2702513888888889}]]
                          ["Fim de Turno"
                           [{:hours 2.422464722222222}]]
                          ["Motor Ocioso Sem Apontamento"
                           [{:hours 0.6835369444444445}]]
                          ["Parada Sem Apontamento"
                           [{:hours 0.32365083333333333}]]
                          ["Pulverização"
                           [{:hours 2.1468525}]]]]]]]]
                    ["Osvaldo Paulista Rodrigues"
                     [{:hours 12.0}
                      [["20210305"
                        [{:hours 12.0}
                         [["Abastecimento de Calda"
                           [{:hours 0.4297447222222222}]]
                          ["Auto Deslocamento"
                           [{:hours 0.56082}]]
                          ["Clima"
                           [{:hours 5.759536388888889}]]
                          ["Fim de Turno"
                           [{:hours 2.2672097222222223}]]
                          ["Limpeza de Bico e Filtro"
                           [{:hours 1.0802316666666667}]]
                          ["Parada Sem Apontamento"
                           [{:hours 0.07162777777777778}]]
                          ["Pulverização"
                           [{:hours 0.8319555555555556}]]
                          ["Refeição"
                           [{:hours 0.9988741666666666}]]]]]]]]
                    ["Ricardo Barbosa"
                     [{:hours 12.0}
                      [["20210305"
                        [{:hours 12.0}
                         [["Aguardando Ordem"
                           [{:hours 4.034454444444444}]]
                          ["Auto Deslocamento"
                           [{:hours 0.04411333333333333}]]
                          ["Clima"
                           [{:hours 3.5573083333333333}]]
                          ["Fim de Turno"
                           [{:hours 4.364123888888889}]]]]]]]]]]]]]]]
             (data90/tree
               [#:data90 {:group-by :site_name}
                #:data90 {:group-by :shift_name}
                #:data90 {:group-by :operator_name}
                #:data90 {:group-by :context_date}
                #:data90 {:group-by :operation_name}]
               [[:hours :timestamp_delta_as_hour :sum]]
               dataset1))))))

(deftest tree-summary-test
  (is (= {:x-sum 10}
         (data90/tree-summary
           (data90/tree
             [#:data90 {:group-by :operation_name}]
             [[:x-sum :x :sum]]
             [{:a "Doing A" :x 3}
              {:a "Doing B" :x 7}]))))

  (is (= {:hours-sum 72.0}
         (data90/tree-summary
           (data90/tree
             [#:data90 {:group-by :operation_name}]
             [[:hours-sum :timestamp_delta_as_hour :sum]]
             dataset1))))

  (is (= {:min 1}
         (data90/tree-summary
           [[:min nil :min]]
           {"Foo" [{:min 1}]
            "Bar" [{:min 2}]})))

  (is (= {:max 2}
         (data90/tree-summary
           [[:max nil :max]]
           {"Foo" [{:max 1}]
            "Bar" [{:max 2}]}))))