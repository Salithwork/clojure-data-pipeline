(ns pipeline.transactional-boundaries
  (:require [next.jdbc.sql :as sql]
            [pipeline.db :as db]
            [next.jdbc :as jdbc]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [cheshire.core :as json]))

;; 1 . Establish a database connection and defining the connection parameters for SQLite database or global database connection or global data source map specification variable. This will be used for executing SQL queries and transactions in the data ingestion pipeline.
(def ds (jdbc/get-datasource {:dbtype "sqlite" :dbname "target/production_pipeline.db"}))

(defn initialize-relational-schema! [db-source]
  (jdbc/with-transaction [tx db-source]
    ;; 1.Forging the master products metadata schema
    (jdbc/execute! tx ["CREATE TABLE IF NOT EXISTS products (product_id TEXT PRIMARY KEY, price REAL);"])
    ;; 2.Forge the transactional Order Items bridging junction schema
    (jdbc/execute! tx ["CREATE TABLE IF NOT EXISTS order_items (item_id TEXT PRIMARY KEY, order_id TEXT, product_id TEXT, price_paid REAL);"])
    ;; 3. MISSING LINK: Operational Metadata Tracking Schema
    (jdbc/execute! tx ["CREATE TABLE IF NOT EXISTS ingestion_log (log_id INTEGER PRIMARY KEY AUTOINCREMENT, File_name TEXT, executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, status TEXT);"])))

(defn execute-bulk-test! []
  (jdbc/with-transaction [tx ds]
    (let [sql-stmt "INSERT OR REPLACE INTO ingestion_log (log_id, status) VALUES (?, ?)"
          row-data [["batch_4" "success"] ["batch_5" "success"] ["batch_6" "failed"]]]
      (jdbc/execute-batch! tx sql-stmt row-data {}))))

(execute-bulk-test!)

(defn line->batch-row [line]
  (let [Index (str/split (str/trim line) #"\s*,\s*")]
    [(nth Index 0) (nth  Index 1)]))

(defn file->batch-matrix [file-path]
  (with-open [reader (clojure.java.io/reader file-path)]
    (doall (map line->batch-row (line-seq reader)))))

(spit "target/stream_test.csv" "1,B_500,success\n2,B_501,failed\n3,B_502,success")

(file->batch-matrix "target/stream_test.csv") ;; This should return a vector of vectors representing the rows in the CSV file

(defn stream-file-to-database! [file-path database-source]
  (initialize-relational-schema! database-source)
  (with-open [reader (clojure.java.io/reader file-path)]
    (let [matrix (doall (map line->batch-row (line-seq reader))) sql-template "INSERT OR IGNORE INTO ingestion_log (log_id, status) VALUES (?, ?)"]
      (jdbc/with-transaction [tx database-source]
        (jdbc/execute-batch! tx sql-template matrix {}))))

  (jdbc/execute! ds ["DROP TABLE IF EXISTS ingestion_log;"]);;;; Dropping the table to clear out the incorrect layout schema
  (jdbc/execute! ds ["CREATE TABLE IF NOT EXISTS ingestion_log (log_id INTEGER PRIMARY KEY AUTOINCREMENT, File_name TEXT, executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, status TEXT);"]);;;; Recreating the table with the correct layout schema

  (let [matrix (file->batch-matrix file-path) sql-template "INSERT OR IGNORE INTO ingestion_log (log_id, status) VALUES (?, ?)"]
    (jdbc/with-transaction [tx database-source]
      (jdbc/execute-batch! tx sql-template matrix {}))))

(stream-file-to-database! "target/stream_test.csv" ds)

(jdbc/execute! ds ["SELECT * FROM ingestion_log WHERE id LIKE 'B_%';"])

(initialize-relational-schema! ds)

;; Dual-Table Transaction Boundary 
(defn insertional-relational-records! [tx row-data]
  (let [{:keys [item-id order-id product-id price]} row-data]
    (jdbc/execute! tx ["INSERT OR IGNORE INTO products (product_id, price) VALUES (?, ?);" product-id price])
    (jdbc/execute! tx ["INSERT INTO order_items (item_id, order_id, product_id, price_paid) VALUES (?, ?, ?, ?);" item-id, order-id, product-id, price])))

(defn execute-secure-pipeline-split! [database-source]
  (jdbc/with-transaction [tx database-source]
    (insertional-relational-records! tx {:item-id "item_777" :order-id "order_999" :product-id "product_KETO_SHAKE" :price 29.99})))

(execute-secure-pipeline-split! ds)

(jdbc/execute! ds ["SELECT * FROM order_items;"])

(jdbc/execute! ds ["SELECT * FROM order_items Where item_id = 'item_777';"])

;; The Multiple Table Batch Data Assembly and Insertion Pipeline with Transactional Boundaries
(defn map->batch-matrices [records]
  (let [;; 1.Shape Matrix A: Custom parameters for the products catalog table 
        prod-matrix (map (fn [{:keys [product-id price]}] [product-id price]) records)
        ;; 2.Shape Matrix B: Custom parameters for order items junction table
        item-matrix (map (fn [{:keys [item-id order-id product-id price]}] [item-id order-id product-id price]) records)]

    {:products prod-matrix
     :order-items item-matrix}))

(def sample-maps [{:item-id "I_1" :order-id "0_1" :product-id "P_1" :price 10.0}
                  {:item-id "I_2" :order-id "0_2" :product-id "P_2" :price 20.0}])

(map->batch-matrices sample-maps)

;;The execute-multi-table-batch! function is designed to handle the insertion of data into multiple tables within a single transaction.
;;It takes a data source and a collection of records, transforms them into batch matrices, and executes the necessary SQL statements to insert the data into the respective 
;;tables.The use of transactions ensures that either all operations succeed or none do, maintaining data integrity.

(defn execute-multi-table-batch! [database-source collection-records]
  (let [payloads (map->batch-matrices collection-records)
        sql-p  "INSERT OR IGNORE INTO products (product_id, price) VALUES (?, ?);"
        sql-i  "INSERT INTO order_items (item_id, order_id, product_id, price_paid) VALUES (?, ?, ?, ?);"]
    (jdbc/with-transaction [tx database-source]
      (jdbc/execute-batch! tx sql-p (:products payloads) {})
      (jdbc/execute-batch! tx sql-i (:order-items payloads) {}))))

;; 1. Define a brand new, unique dataset payload map
(def fresh-batch [{:item-id "I_900" :order-id "O_900" :product-id "P_900" :price 99.0}
                  {:item-id "I_901" :order-id "O_901" :product-id "P_901" :price 150.0}])

;; 2. Run your high-speed multi-table batch engine against it
(execute-multi-table-batch! ds fresh-batch)

;;A fast selection query to prove the fresh items landed on disk
(jdbc/execute! ds ["SELECT * FROM order_items WHERE item_id LIKE 'I_9%';"])

;;Master Bridge for multi-table batch

(defn csv-line->record-map [line]
  (let [tokens (str/split (str/trim line) #"\s*,\s*")]
    {:item-id    (nth tokens 0)
     :order-id   (nth tokens 1)
     :product-id (nth tokens 2)
     :price (Double/parseDouble (nth tokens 3))}))

(defn stream-file-to-relational-batch! [file-path database-source]
  (with-open [rdr (io/reader file-path)]
    (let [records (doall (map csv-line->record-map (line-seq rdr)))]
      (execute-multi-table-batch! database-source records))))

(spit "target/relational_stream.csv" "I_950,O_950,PROD_MED_A,85.50\nI_951,O_951,PROD_MED_B,120.00")

(stream-file-to-relational-batch! "target/relational_stream.csv" ds)

(defn validate-row-record [record]
  (let [{:keys [item-id product-id price]} record]
    (and (not (str/blank? item-id))
         (not (str/blank? product-id))
         (and (number? price))
         (and (pos? price)))))

(validate-row-record {:item-id "I_1" :product-id "P_1" :price 10})
(validate-row-record {:item-id ""    :product-id "P_1" :price 10.50})
(validate-row-record {:item-id "I_1" :product-id "P_1" :price -5.00})   

(defn partition-records [records]
  (let [grouped-map (group-by validate-row-record records)]
    {:valid (get grouped-map true [])
     :invalid (get grouped-map false [])}))

(def mixed-batch [{:item-id "I_A" :order-id "O_A" :product-id "P_A" :price 10.0}   ; Valid
                  {:item-id ""    :order-id "O_B" :product-id "P_B" :price 20.0}   ; Invalid (Empty ID)
                  {:item-id "I_C" :order-id "O_C" :product-id "P_C" :price -5.0}])  ; Invalid (Negative price)

(partition-records mixed-batch)

(defn write-quarantine-log! [invalid-rows]
  (when (seq invalid-rows)
             (let [output-path "target/quarantine_records.json"
                   json-data (json/generate-string invalid-rows {:pretty true})]
               (spit output-path json-data))))

(let [split-map (partition-records mixed-batch)]
  (write-quarantine-log! (:invalid split-map)))

(slurp "target/quarantine_records.json")


(defn stream-file-to-defensive-batch! [file-path database-source] 
  (with-open [rdr (io/reader file-path)]
    (let [records (doall (map csv-line->record-map (line-seq rdr)))
          split-map (partition-records records)]
          (write-quarantine-log! (:invalid split-map))
          (when (seq (:valid split-map))
            (execute-multi-table-batch! database-source (:valid split-map))))))

(spit "target/dirty_stream.csv" "I_990,O_990,PROD_ALPHA,45.00\n,O_991,PROD_BETA,25.00")

(jdbc/execute! ds ["SELECT * FROM order_items WHERE item_id = 'I_990';"])

(stream-file-to-defensive-batch! "target/dirty_stream.csv" ds)

(slurp "target/quarantine_records.json")

;;07/08/2026

(defn calculate-throughput! [row-count start-nano]
  (let [end-nano     (System/nanoTime)
        elapsed-sec  (/ (- end-nano start-nano) 1000000000.0)
        rows-per-sec (if (pos? elapsed-sec) (/ row-count elapsed-sec) 0.0)]
    (println (str "Engine Throughput: " rows-per-sec " rows/sec (Time: " (* elapsed-sec 1000.0) " ms)"))
    row-count))

(spit "target/dirty_stream.csv" "I_9999,O_9999,PROD_ALPHA,45.00\n,O_9999,PROD_BETA,25.00")

(stream-file-to-defensive-batch! "target/dirty_stream.csv" ds)

(defn calculate-throughput! [row-count start-nano]
  (let [end-nano     (System/nanoTime)
        elapsed-sec  (/ (- end-nano start-nano) 1000000000.0)
        rows-per-sec (if (pos? elapsed-sec) (/ row-count elapsed-sec) 0.0)]
    {:rows-processed row-count
     :elapsed-ms     (* elapsed-sec 1000.0)
     :throughput     rows-per-sec})) ; Returns map data directly!

(defn stream-file-to-defensive-batch! [file-path database-source]
  (let [start-clock (System/nanoTime)]
    (with-open [rdr (clojure.java.io/reader file-path)]
      (let [records   (doall (map csv-line->record-map (line-seq rdr)))
            split-map (partition-records records)
            good-rows (:valid split-map)]
        (write-quarantine-log! (:invalid split-map))
        (when (seq good-rows)
          (execute-multi-table-batch! database-source good-rows))

        ;; Return the final throughput calculation map out of the entire pipeline block!
        (calculate-throughput! (count records) start-clock)))))

(spit "target/dirty_stream.csv" "I_11111,O_11111,PROD_ALPHA,45.00\n,O_11111,PROD_BETA,25.00")

(stream-file-to-defensive-batch! "target/dirty_stream.csv" ds)

