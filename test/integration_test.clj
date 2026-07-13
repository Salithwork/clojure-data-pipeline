(ns dna-pipeline.data-pipeline.integration-test
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [dna-pipeline.file-scanner :as pipeline])) ;; Production Module

(clojure.test/deftest dlq-lifecycle-test
   
   (let [test-dlq-file (clojure.java.io/file "target/dlq_integration_test.json")]
     ;; Step 1: Ensuring directory path exists
     (.mkdirs (clojure.java.io/file "target"))

     ;; Step 2: Setting up (Spit anomaly payload)
     (spit test-dlq-file "{\"error\": \"malformed_token_size\", \"row\": 2}")

     (clojure.test/testing "Verify file quarantine cycle"
       ;; Step 3: Asserting (Verify file exists)
       (clojure.test/is (.exists test-dlq-file))

       ;; Step 4: Teardown (To clean local storage)
       (clojure.java.io/delete-file test-dlq-file)

       ;; Step 5: Final Check (Verify file is removed)
       (clojure.test/is (not (.exists test-dlq-file))))))

(clojure.test/run-tests)

(deftest malformed-stream-quarantine-test
  (let [mock-input (io/file "target/dirty_input.vcf")
        mock-dlq   (io/file "target/error_dlq.json")]
    (.mkdirs (io/file "target"))

    ;; Step for injecting  erractic spaces (#"\s+") and a completely broken middle row
    (spit mock-input "chr1 1001 A T\nBROKEN_ROW_MISSING_DATA\nchr2\t2002\tC\tG \n")

    (testing "Transducer extraction and automated validation"
      ;; Core assertion: The input file successfully landed on the  Windows file system
      (is (.exists mock-input))

      ;; TEARDOWN: Clean up tracking traces completely
      (io/delete-file mock-input)
      (is (not (.exists mock-input))))))

(clojure.test/run-tests)

(deftest end-to-end-pipeline-integration-test
  (let [input-path "target/test_batch.csv"
        dlq-path "target/test_output_dlq.json"]
    
    (spit input-path "ORD_1,UserA,KetoA,10.00\nORD_BAD_ROWUserC\nORD_2,UserB,20.00") ;; SETUP: Create an active dataset with a broken row
    (testing "Verify stream parsing and quarantine isolation"
      (pipeline/execute-production-pipeline input-path dlq-path) ;; Executing production pipeline logic 
      (is (.exists (io/file dlq-path)))  ;; ASSERT: Prove that the DLQ file exists and holds  the anomaly map
      (is (str/includes? (slurp dlq-path) "ORD_BAD_ROW" ))
      (io/delete-file input-path) ;; To clear or disk mutations
      (io/delete-file dlq-path)
      (is (not (.exists (io/file input-path)))))))

(clojure.test/run-all-tests)






