(ns user
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.pprint :as pprint]
            [clojure.tools.namespace.repl :refer [refresh]]

            [data90.core :as data90]))

(comment

  (def A
    "Operações 'automáticas' configuradas nos parâmetros do sistema."
    #{"Auto Deslocamento"
      "Motor Ocioso Sem Apontamento"
      "Parada Sem Apontamento"
      "Desligamento Sem Apontamento"})


  (def olap-result
    (json/read (io/reader (io/resource "jcn_su4_olap.json")) :key-fn keyword))

  (def dataset
    (->> (:rows olap-result)
         (filter :is_leaf)
         (map
           (fn [row]
             (let [row (dissoc row :d :is_leaf)

                   ;; Quantidade de horas nessa Operação.
                   timestamp_delta_as_hour (:timestamp_delta_as_hour row)]

               ;; Adiciona duas novas keys para 'horas manuais' e 'horas automáticas'.
               ;; Se a Operação é manual, a diferença de horas é atribuída as horas manuais.
               ;; E se a Operação é automática, a diferença de horas é atribuída as horas automáticas.
               (if ((comp A :operation_name) row)
                 (assoc row
                   :manual_hours 0
                   :auto_hours timestamp_delta_as_hour)
                 (assoc row
                   :manual_hours timestamp_delta_as_hour
                   :auto_hours 0)))))))

  (group-by (juxt :operator_name) rows)
  (group-by (juxt :operator_name :operation_name) rows)



  ;; OPERAÇÃO       OPERADOR    HORAS
  ;; ---------------------------------
  ;; Pulverização               10
  ;; Pulverização   Carlinhos   10
  ;; Pulverização   Paulinho    3


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


  ;; AGREGAÇÃO
  ;; Precisamos saber quais são as medidas, e para cada uma a função (de agregação) que deve ser aplicada.
  ;; Nesse exemplo do Carlinhos e Paulinho, temos apenas uma medida: quantidade de horas.
  ;; Portanto, para esse caso poderíamos definir as medidas nesse formato:
  [[:hours :sum]]

  ;; Além das medidas, também precisamos das dimensões.
  ;; Talvez não faça sentido pensar em dimensões aqui,
  ;; mas apenas em um conjunto de colunas para seleção.
  ;; Para o nosso exemplo:
  [:operator_name]

  ;; Pensando um pouco mais, talvez a gente consiga pensar em um formato
  ;; para definir a agregação.
  ;; Ao invés de seperar medidas e dimensões,
  ;; podemos definir no mesmo formato:
  (def aggregate-spec
    [[:operation_name :select]
     [:auto_hours :sum]
     [:manual_hours :sum]])

  (clojure.pprint/pprint
    (data90/tree aggregate-spec dataset))

  (= {:total-x 15, :total-y 15}
     (data90/aggregate3
       [[:total-x :x :sum]
        [:total-y :y :sum]]
       [{:x 10 :y 5}
        {:x 5 :y 10}]))


  (data90/tree-group
    [:operator_name]
    [[:hours-sum :hours :sum]]
    dataset)

  (keys *1)

  ;; =>

  {[:operator_name "Carlinhos"]
   [{:operator_name "Carlinhos"
     :hours 30}]

   [:operator_name "Paulinho"]
   [{:operator_name "Paulinho"
     :hours 25}]}


  ;; ---

  {[:operator_name "Carlinhos"]
   [{:operator_name "Carlinhos"
     :hours 30}

    {[:date "2021-03-05"]
     [{:operator_name "Carlinhos"
       :date "2021-03-05"
       :hours 20}]

     [:date "2021-03-04"]
     [{:operator_name "Carlinhos"
       :date "2021-03-04"
       :hours 25}]}]}

  ;; ---


  {[:operator_name "Carlinhos"]
   [{:operator_name "Carlinhos"
     :hours 30}

    {[:date "2021-03-05"]
     [{:operator_name "Carlinhos"
       :date "2021-03-05"
       :hours 20}

      {[:operation_name "Adubação"]
       [{:operator_name "Carlinhos"
         :date "2021-03-05"
         :operation_name "Adubação"
         :hours 15}]

       [:operation_name "Pulverização"]
       [{:operator_name "Carlinhos"
         :date "2021-03-05"
         :operation_name "Pulverização"
         :hours 5}]}]}]}

  )