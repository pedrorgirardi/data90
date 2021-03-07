(ns data90.core-test
  (:require [clojure.test :refer :all]
            [data90.core :as data90]))

(deftest aggregate-test
  (testing "Sum"
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
               rows))))))

(deftest tree-group-test
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

    (is (= {"Adubação"
            [{:sum 15}]

            "Pulverização"
            [{:sum 10}]}

           (data90/tree-group
             [:operation_name]
             [[:sum :hours :sum]]
             dataset)))

    (is (= {"Carlinhos"
            [{:sum 22}]

            "Paulinho"
            [{:sum 3}]}

           (data90/tree-group
             [:operator_name]
             [[:sum :hours :sum]]
             dataset)))

    (is (= {["O1" "Paulinho"]
            [{:sum 3}]

            ["O2" "Carlinhos"]
            [{:sum 22}]}

           (data90/tree-group
             [(juxt :operator_code :operator_name)]
             [[:sum :hours :sum]]
             dataset)))

    (is (= {"Carlinhos"
            [{:hours 22}
             {"Adubação"
              [{:hours 15}]

              "Pulverização"
              [{:hours 7}]}]

            "Paulinho"
            [{:hours 3}
             {"Pulverização"
              [{:hours 3}]}]}

           (data90/tree-group
             [:operator_name :operation_name]
             [[:hours :hours :sum]]
             dataset)))

    (is (= {"Carlinhos"
            [{:hours 22}
             {"2021-03-05"
              [{:hours 7}
               {"Pulverização"
                [{:hours 7}]}]

              "2021-03-06"
              [{:hours 15}
               {"Adubação"
                [{:hours 15}]}]}]

            "Paulinho"
            [{:hours 3}
             {"2021-03-05"
              [{:hours 3}
               {"Pulverização"
                [{:hours 3}]}]}]}

           (data90/tree-group
             [:operator_name :date :operation_name]
             [[:hours :hours :sum]]
             dataset)))))
