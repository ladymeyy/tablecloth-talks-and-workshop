(ns tablecloth-workshop.core
  (:require
    [tablecloth.api :as tc]
    [tech.v3.dataset.print :as print]
    [tech.v3.datatype.functional :as fun]
    [clojure.string :as string]))


(def pokemon-ds-path "resources/pokemon.csv")
(def clean-pokemon-ds-path "resources/clean-pokemon.csv")
(def combats-ds-path "resources/combats.csv")



; 1.Load the csv
; 2.Clean the data
; 3. present the table

(-> pokemon-ds-path
    (slurp)
    (string/replace #"'" "")
    (->>(spit clean-pokemon-ds-path)))

(def pokemon-ds (-> pokemon-ds-path
                    (tc/dataset {:key-fn keyword})))



(def combats-ds (-> combats-ds-path
                    (tc/dataset {:key-fn keyword})))


;Select specific columns
(-> pokemon-ds
    (tc/select-columns [:pokedex_number :name :height_m :weight_kg :generation :is_legendary]))


;Find shortest pokemon
(-> pokemon-ds
    (tc/select-columns [:pokedex_number :name :height_m :weight_kg :generation :is_legendary])
    (tc/order-by [:height_m]))

;Find tallest pokemon
(-> pokemon-ds
    (tc/select-columns [:pokedex_number :name :height_m :weight_kg :generation :is_legendary])
    (tc/order-by [:height_m] :desc))




;Which pokemon are you?
;--------------
;SELECT pokedex_number, name, height_m, weight_kg
;FROM  p pokemon.csv
;LEFT JOIN (SELECT COUNT(winner) winner
;               FROM battles
;               GROUP BY winner
;               ) AS b WHERE b.winner = p.pokedex_number
;WHERE p.height BETWEEN height-0.5 AND height+0.5
;AND p.weight BETWEEN weight-0.5 AND  weight+0.5


;User info
(def user-height 0.5)
(def user-weight 50)



;These functions won't check for NULL values - produces an error
(defn weight-in-range? [row]
  (> (+ user-weight 1) (:weight_kg row) (- user-weight 1)))

(defn height-in-range? [row]
  (> (+ user-height 0.5) (:height_m row) (- user-height 0.5)))

;Get a row from csv as a map and check if weight & height is in pre-defined range
;This also checks null value.
(defn weight-in-range? [row]
  (and (:weight_kg row)
       (> (+ user-weight 1) (:weight_kg row) (- user-weight 1))))

(defn height-in-range? [row]
  (and (:height_m row)
       (> (+ user-height 0.5) (:height_m row) (- user-height 0.5))))


;Get pokemon in height range
(-> pokemon-ds
    (tc/select-columns [:pokedex_number :name :height_m :weight_kg ])
    (tc/select-rows height-in-range?)
    )


;Get pokemon in height and weight range
(-> pokemon-ds
    (tc/select-columns [:pokedex_number :name :height_m :weight_kg ])
    (tc/select-rows (fn [row]
                      (and (weight-in-range? row) (height-in-range? row))))
    )


;group by winner
(-> combats-ds
    (tc/group-by [:Winner])
    )


;Adds another column to the table: Each row holds the size of the group it belongs to
(-> combats-ds
    (tc/group-by [:Winner])
    (tc/aggregate {:number-of-wins tc/row-count} )
    )


;Join will be done by with matching columns by their name so rename :winner to :pokedex_number
(def number-of-wins
  (-> combats-ds
      (tc/group-by [:Winner])
      (tc/aggregate {:number-of-wins tc/row-count} )
      (tc/rename-columns {:Winner :pokedex_number})
      ))

(def pokemon-filtered
  (-> pokemon-ds
      (tc/select-columns [:pokedex_number :name :height_m :weight_kg ])
      (tc/select-rows (fn [row]
                        (and (weight-in-range? row) (height-in-range? row))))
      (tc/left-join number-of-wins :pokedex_number)))


