(ns clj-spark-apps.core-test
  (:require [clojure.test :refer :all]
            [clj-spark-apps.core :refer :all]
            [sparkling.core :as spark]
            [sparkling.conf :as conf]
            [sparkling.destructuring :as s-de]
            [sparkling.serialization]))


;; ------------------
;; -    FIXTURES    -
;; ------------------

;; -------------------
;; -      TESTS      -
;; -------------------

(deftest domain-functions-test
  (testing "tf functions"
    (is (= ["quick" "brown" "fox" "jumps"]
         (remove-stopwords (terms "A quick brown fox jumps"))
         )))

  (testing "idf functions"
    (is (= -0.8109302162163288 (idf 4 8)))
    (is (= -0.2231435513142097 (idf 4 4)))
    (is (= 1.3862943611198906 (idf 4 0)))))
