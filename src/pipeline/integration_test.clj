(ns pipeline.integration-test
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.java.io :as io]
            [next.jdbc :as jdbc]
            [pipeline.transactional-boundaries :as src]
            [cheshire.core :as json]
            [clojure.string :as str]
            [next.jdbc.sql :as sql]
            [pipeline.db :as db]
            [clojure.spec.alpha :as s]
            [scratchpad.scratchpad :as utilis]))

(deftest end-to-end-relational-schema-test
  (let [test-csv "target/test_orders.csv"
        test-db "target/test_relational_storage.db"
        ;; for an isolated sandbox link for the test run
        test-ds (jdbc/get-datasource {:dbtype "sqlite" :dbname test-db})]

    (testing "Verify relational multi-table schema depoly safely"
      ;; For running the production logic against isolated testing database pointer!
      (src/initialize-relational-schema! test-ds)

      ;; Assert that both tables were physically constructed inside the test database file
      (let [prod-table-check (jdbc/execute! test-ds ["SELECT * FROM products;"])
            item-table-check (jdbc/execute! test-ds ["SELECT * FROM order_items;"])]
        (is (= 0 (count prod-table-check)))
        (is (= 0 (count item-table-check)))))

    ;; Cleaning up
    (io/delete-file test-csv true)
    (io/delete-file test-db true)))

;; 6. Run the test suite execution command cleanly outside the block
(clojure.test/run-tests 'pipeline.integration-test)

(deftest end-to-end-file-to-relational-batch-test
  (let [test-csv "target/test_relational_stream.csv"
        test-db "target/test_batch_storage.db"
        test-ds (jdbc/get-datasource {:dbtype "sqlite", :dbname "test-db"})]

    (spit test-csv "I_1001,O_1001,PROD_X,45.00\nI_1002,O_1002,PROD_Y,60.50")

    (testing "Verify lazy file streaming flushes cleanly to multi-table batch targets"
      (src/initialize-relational-schema! test-ds)
      (src/stream-file-to-relational-batch! test-csv test-ds)
      (let [products (jdbc/execute! test-ds ["SELECT * FROM products;"])
            items    (jdbc/execute! test-ds ["SELECT * FROM order_items;"])]
        (is (= 2 (count products)))
        (is (= 2 (count items)))
        (is (= (:order_items/item_id (first items)) "I_1001"))))

    (io/delete-file test-csv true)
    (io/delete-file test-db true)))

(clojure.test/run-test end-to-end-file-to-relational-batch-test)

(deftest end-to-end-defensive-firewall-test
  (let [test-csv "target/test_dirty_stream.csv"
        test-db  "target/test_defensive_storage.db"
        test-ds  (jdbc/get-datasource {:dbtype "sqlite", :dbname test-db})
        test-json "target/quarantine_records.json"]

    (spit test-csv "I_TEST_OK,O_1,PROD_A,10.00\n,O_2,PROD_B,20.00")

    (testing "Verify data firewall intercepts anomalies and logs to quarantine JSON"
      (src/initialize-relational-schema! test-ds)
      (src/stream-file-to-defensive-batch! test-csv test-ds)
      (let [db-records (jdbc/execute! test-ds ["SELECT * FROM order_items;"])
            json-text (slurp test-json)]
        (is (= 1 (count db-records)))
        (is (= (:order_items/item_id (first db-records)) "I_TEST_OK"))
        (is (str/includes? json-text "PROD_B"))))

    (io/delete-file test-csv true)
    (io/delete-file test-db true)
    (io/delete-file test-json true)))

(clojure.test/run-test end-to-end-defensive-firewall-test)
;;07/09/2026

(deftest structural-telemetry-test
  (testing "Telemetry map conforms to strict numeric infracstucture primitives"
    (let [sample-ctx {:db-spec {:class-name "org.sqlite.JDBC" :subname "data.db"}}
          mock-db-row {:id 101 :throughput 85.5 :elapsed-ms 12.4}
          processed-telemetry (assoc mock-db-row :log_id (:id mock-db-row))]
      (is (contains? processed-telemetry :id))
      (is (number? (:id processed-telemetry)))
      (is (number? (:throughput processed-telemetry))
      (is (number? (:elapsed-ms processed-telemetry)))))))
          
(clojure.test/run-test structural-telemetry-test)

(defn commit-log-batch! [datasource batch-payloads]
  ;1 Opening secure, protected database transaction bubble
  (jdbc/with-transaction [tx-con datasource]
    ;2 Run verified loop over the batch box
    (doseq [payload batch-payloads]
      (if (s/valid? :pipeline/log-entry payload)
        ;; Extract, flatten the #uuid object, and bind the clean map shape
        ;; Path A. Modern next.jdbc single execution syntax
        (let [clean-payload (utilis/normalize-payload payload)]
        ;; The perfectly flat, safe text payload to SQlite
        (sql/insert! tx-con :telemetry_logs clean-payload))
        ;; Path B Rollback trigger
        (throw (ex-info "Transaction Aborted: Corrupt record encountered." payload))))))


         