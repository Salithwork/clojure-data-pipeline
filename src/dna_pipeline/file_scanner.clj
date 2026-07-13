(ns dna-pipeline.file-scanner
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [cheshire.core :as json]
            [next.jdbc :as jdbc])) ;;Use the clean alias mapping 'jdbc' here


(defn find-json-logs [dir-path]
  (let [folder (io/file dir-path)]
    (if (.exists folder)
      (->> (.listFiles folder)
           (filter #(.isFile %))
           (map #(.getName %))
           (filter #(str/ends-with? % ".json")))
      [])))

(defn process-anomaly-payload [file-path]
  (let [target-file (io/file file-path)]
    (if (.exists target-file)
      (with-open [rdr (io/reader target-file)]
        (json/parse-stream rdr true)) nil)))

(defn process-all-logs [dir-path]
  (let [all-files (find-json-logs dir-path)]
    (run! (fn [all-files]
          (let [full-path (str dir-path "/" all-files)]
            (println "Found Error Details:" (process-anomaly-payload full-path))))
    all-files)))

(defn parse-ecommerce-row [raw-line]
  (let [ clean-line (str/trim raw-line)
        tokens (str/split clean-line #"\s*,\s*")]
    (if (= (count tokens) 4)
      {:order-id (nth tokens 0)
       :name (nth tokens 1)
       :item (nth tokens 2)
       :cost (Double/parseDouble (nth tokens 3))}
      {:status :failed-validation :line raw-line}
      )))

(defn process-ecommerce-batch-file [input-path] 
  (let [source-file (io/file input-path)] 
    (if (.exists source-file) 
      (with-open [rdr (io/reader source-file)] 
        (let [all-lines (line-seq rdr)] 
          (doall (map parse-ecommerce-row all-lines)))) 
      :error-file-not-found)))

(defn log-anomaly-to-disk [dlq-path anomaly-map]
  (let [json-line-string (str (json/generate-string anomaly-map) "\n")]
    (with-open [wtr (io/writer dlq-path :append true)]
      (.write wtr json-line-string))))

(defn execute-production-pipeline [input-file dlq-path]
  (let [source-path (clojure.java.io/file input-file)]
    (if (.exists source-path)
      (with-open [rdr (clojure.java.io/reader source-path)]
        (run! (fn [raw-line]                                                  ;; Loop line-by-line
                (let [record (parse-ecommerce-row raw-line)]                  ;; Parsing raw into map
                  (if (= (:status record) :failed-validation)                 ;; Conditional Routing check
                    (log-anomaly-to-disk dlq-path record)                     ;; Endpoint A: Saving Anomaly to DLQ File
                    (println "[SUCCESS] Clean Order Processed:" record))))    ;; Endpoint B: Clean Stream Output
              (line-seq rdr))))))                                               ;; Feed line sequence into loop  
      

(defn extract-anomaly-severity [raw-json-string]
  (let [data-map (json/parse-string raw-json-string true)]
    (get-in data-map [:metadata :error-metrics :severity])))

(extract-anomaly-severity "{\"metadata\": {\"error-metrics\": {\"severity\": \"CRITICAL\"}}}")

(defn process-nested-alert [incoming-json-string]
  ;; 1. Convert the raw text string into a clean Clojure map object first
  (let [parsed-payload (cheshire.core/parse-string incoming-json-string true)
        severity-level (get-in parsed-payload [:metadata :error-metrics :severity])]

    (if (= severity-level "CRITICAL")
      ;; 2. Pass a clean, unified data map layout to your disk writer
      (log-anomaly-to-disk "target/critical_alerts.json" {:status "URGENT_ALERT" :data parsed-payload})
      (log-anomaly-to-disk "target/standard_errors.json" {:status "NORMAL_LOG" :data parsed-payload}))))


(defn create-orders-table! []
  ;; 1. Defensive Shield: Ensure the parent 'target' directory exists
  (.mkdirs (io/file "target"))

  (let [db-url "jdbc:sqlite:target/production_pipeline.db"]
    ;; 2. Explicit Class reference: Use the complete absolute class name layout path
    (with-open [conn (java.sql.DriverManager/getConnection db-url)]
      ;; 3. Safely execute the raw SQL string statement
      (jdbc/execute! conn ["CREATE TABLE IF NOT EXISTS orders (id TEXT, name TEXT, item TEXT, cost REAL);"]))))

;; Test Run A: Should trigger the critical path
(process-nested-alert "{\"metadata\": {\"error-metrics\": {\"severity\": \"CRITICAL\"}}}")

;; Test Run B: Should trigger the standard fallback path
(process-nested-alert "{\"metadata\": {\"error-metrics\": {\"severity\": \"LOW\"}}}")

(execute-production-pipeline "target/mock_order.csv" "target/production_dlq.json")

(log-anomaly-to-disk "target/live_dlq.json" {:error "Missing field", :line "BAD_ROW_1"})

(log-anomaly-to-disk "target/live_dlq.json" {:error "Invalid price", :line "BAD_ROW_2"})

(slurp "target/live_dlq.json")
(slurp "target/production_dlq.json")

(spit "target/mock_order.csv" "ORD_1,Ajay,KetoGummies,29.95\nORD_2,Reema,KetoCollagen,45.00\nORD_BROKEN_LINE_SPACE\nORD_3,Kabir,KetoProtein,89.99")

(process-ecommerce-batch-file "target/mock_order.csv")

(parse-ecommerce-row "ORDER_999 , Vijay  , KetoProtein , 59.95 ")

(parse-ecommerce-row "999, Vijay, KetoProtein, 59.95")

(find-json-logs "target")

(spit "target/permanent_error_log.json" "{\"error\": \"test\"}")

(find-json-logs "target")

(process-anomaly-payload "target/permanent_error_log.json")

(process-all-logs "target")

(.exists (io/file "target/mock_orders.csv"))
(.exists (io/file "target/mock_order.csv"))

(slurp "target/critical_alerts.json")

(in-ns 'dna-pipeline.file-scanner)
