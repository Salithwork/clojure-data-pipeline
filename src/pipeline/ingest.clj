(ns pipeline.ingest
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [next.jdbc.sql :as sql]
            [pipeline.db :as db]
            [clojure.string :as str]))

(defn row->order-map [row]
  {:order-id (str/trim (nth row 0))
   :customer_name (str/trim (nth row 1))
   :total (Double/parseDouble (str/trim (nth row 2)))})

(defn save-order! [order-map]
  (next.jdbc.sql/insert! :processed_orders
                         {:order_id       (:order-id order-map)
                          :customer_name  (:customer_name order-map)
                          :total          (:total order-map)
                          :status        "PROCESSED"}))

(defn process-csv-ingestion! [file-path]
  (with-open [reader (io/reader file-path)]
    (let [csv-data (csv/read-csv reader)
          data-rows (rest csv-data)] ;; 'rest' drops the first row header line
      (doseq [row data-rows]
        (try
          (let [transformed-map (row->order-map row)]
            (db/save-order! transformed-map)
            (println "Successfully persisted record ID:" (:order_id transformed-map)))
          (catch Exception e
            (println "Pipeline Staging Alert: Failed processing row" row "Reason:" (.getMessage e))))))))

(spit "mock_orders.csv"
      (str "order_id,customer_name,total_amount\n"
           "ORD-2026-001, Alpha Biotech, 4500.00\n"
           "ORD-2026-002, KetoMed Labs, 1250.50\n"))

(defn check-database-contents! []
  (next.jdbc.sql/query db/ds ["SELECT * FROM processed_orders"]))

(check-database-contents!)

(defn clear-table! []
  (next.jdbc/execute! pipeline.db/ds ["DELETE FROM processed_orders"]))

(clear-table!)

(defn run-pipeline! []
  (process-csv-ingestion! "mock_orders.csv"))

(next.jdbc/execute! pipeline.db/ds ["DELETE FROM processed_orders"])

(run-pipeline!)

(next.jdbc/execute! pipeline.db/ds ["SELECT * FROM processed_orders"])

(first (rest (clojure.data.csv/read-csv (clojure.java.io/reader "mock_orders.csv"))))

(second (rest (clojure.data.csv/read-csv (clojure.java.io/reader "mock_orders.csv"))))

(require 'pipeline.db' :reload-all)

;; 1. Wipe the old records
(next.jdbc/execute! pipeline.db/ds ["DELETE FROM processed_orders"])

;; 2. Run your pipeline job
(pipeline.ingest/run-pipeline!)

;; 3. Query your data rows back out
(next.jdbc.sql/query pipeline.db/ds ["SELECT * FROM processed_orders"])

(pipeline.db/save-order! {:order_id "DEBUG-001", :customer_name "Direct Test Labs", :total 99.99})

(next.jdbc.sql/query pipeline.db/ds ["SELECT * FROM processed_orders WHERE order_id = 'DEBUG-001'"])

(defn run-primitive-init! []
(let [db-spec { :dbtype "sqlite" :dbname "target/syn_test.db"}
      ds      (next.jdbc/get-datasource db-spec)]
              (next.jdbc/execute! ds ["CREATE TABLE IF NOT EXISTS syn_check (name TEXT);"])))

(run-primitive-init!)



