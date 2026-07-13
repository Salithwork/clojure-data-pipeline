(ns dna-pipeline.parser-test
  (:require [clojure.test :refer [deftest is testing]]
            [dna-pipeline.parser :as parser]))


(deftest parser-success-test
  (testing "Verify that spaced copy-paste variations map cleanly to our target schema keys"
    (let [clean-input   ["chr1 10177 . A AC"]   ;; Hand-type using standard spaces!
          result        (transduce parser/auditing-parser conj clean-input)
          expected-map  {:chromosome "chr1" :position "10177" :id "." :reference "A" :alteration "AC"}]

      (is (= :ok (:type (first result))))
      (is (= expected-map (:payload (first result)))))))

(clojure.test/run-tests)

