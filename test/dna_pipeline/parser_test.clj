(ns dna-pipeline.parser-test
  (:require [clojure.test :refer [deftest is testing]]
            [dna-pipeline.parser :as parser]
            [dna-pipeline.router :as router]  ;; File Stream router 
            [clojure.java.io :as io]))


(deftest parser-suite-test
  (testing "Happy Path: Well-formed VCF row maps cleanly to target schema keys"
    (let [clean-input   ["chr1\t10177\t.\tA\tAC"]
          result        (transduce parser/auditing-parser conj clean-input)
          expected-map  {:chromosome "chr1" :position "10177" :id "." :reference "A" :alteration "AC"}]
      (is (= :ok (:type (first result))))
      (is (= expected-map (:payload (first result))))))

  (testing "Anomaly Path: Rows missing mandatory column attributes are explicitly routed as errors"
    (let [faulty-input ["chr2\t20488\t.\tG"]
          result       (transduce parser/auditing-parser conj faulty-input)
          expected-err {:type :error :payload ["chr2" "20488" "." "G"]}]
      (is (= expected-err (first result))))))

(deftest e2e-file-stream-test
  (testing "Verifying that an end-to-end file ingest accurately routes anomalies to the local disk file system"
    (let [input-path "test_mock_data.vcf"
      output-path "error-dlq.json"]
      ;; Setup to wipe old output files if they exit from yesterday
      (when (.exists (io/file output-path) (io/delete-file output-path)) 
      ;; Setup to write copy-paste error input data dynamically to drive
      (spit input-path "chr2 20488 . G/n") ;; 4 columns instead of 5 (Anomaly row)
      ;; The live disk stream ingestion pipeline
      (router/stream-vcf-file! input-path)
      (is (.exists (io/file output-path))
      (when (.exists (io/file input-path))(io/delete-file input-path)))))

(clojure.test/run-tests 'dna-pipeline.parser-test)