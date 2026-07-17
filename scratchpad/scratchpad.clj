(ns scratchpad.scratchpad
  (:require [clojure.test :refer [is]]
            [clojure.spec.alpha :as s]
            [next.jdbc :as jdbc]
            [clojure.spec.gen.alpha :as gen]
            [next.jdbc.sql :as sql]
            [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.string :as str]))

(defn insert-to-sqlite! [data] :stored-successfully)

(defn save-pipeline-log! [payload]
  (if (s/valid? :pipeline/log-entry payload)
    (insert-to-sqlite! payload)
    (throw (ex-info "Pipeline Write Intercepted: Payload failed schema validation contract."
                    {:explain-data (s/explain-data :pipeline/log-entry payload)}))))


(let [batch-box [{:pipeline/id 101 :pipeline/status "active"}     ;; Good Map
                 {:pipeline/id "Samuel" :pipeline/status "error"}]] ;; Corrupted Map
  ;; This is our data box. Now we can run our conveyor belt on it.
  (println "Data box loaded with records!"))

(let [batch-box [{:pipeline/id 101 :pipeline/status "active"}
                 {:pipeline/id "Samuel" :pipeline/status "error"}]]
  (doseq [payload batch-box]
    (println "Processing item ID:" (:pipeline/id payload))))

(let [batch-box [{:pipeline/id 101 :pipeline/status "active"}
                 {:pipeline/id "Samuel" :pipeline/status "error"}]]
  (doseq [payload batch-box]
    (if (s/valid? :pipeline/log-entry payload)
      (println "Row Clean -Ready for DB Insert")
      (println "Corrupt Data Block! Rollback Triggered"))))

(gen/generate (s/gen :pipeline/log-entry))

(gen/sample (s/gen :pipeline/log-entry) 3)

;; 1. Re-evaluating base rules into the empty JVM memory
(s/def :pipeline/id integer?)
(s/def :pipeline/status string?)

;; 2. Re-evaluating composite map blueprint
(s/def :pipeline/log-entry (s/keys :req [:pipeline/id :pipeline/status]))

;; 3. generator factory!
(gen/sample (s/gen :pipeline/log-entry) 3)

;; 1. Registering individual data component types
(s/def :pipeline/log_id uuid?)
(s/def :pipeline/status string?)
(s/def :pipeline/telemetry integer?)

;; 2. Combine all three components into the required key structure
(s/def :pipeline/log-entry (s/keys :req [:pipeline/log_id :pipeline/status :pipeline/telemetry]))

(def mock-ds {:dbtype "sqlite" :dbname "data/pipeline_storage.db"})

(defn initialize-test-database! [datasource]
  (jdbc/execute! datasource
                 ["CREATE TABLE telemetry_logs (log_id TEXT PRIMARY KEY, Status TEXT, telemetry INTEGER);"]))

(initialize-test-database! mock-ds)

;; Pulling the heavy native JVM #uuid object instance out and cast it to pure text string primitive
(defn normalize-payload [payload]
  (let [raw-uuid (:pipeline/log_id payload)]
    (assoc payload :pipeline/log_id (str raw-uuid))))

(normalize-payload (gen/generate (s/gen :pipeline/log-entry)))

(initialize-test-database! mock-ds)

(def large-stress-batch (gen/sample (s/gen :pipeline/log-entry) 50))

(with-open [conn (jdbc/get-connection mock-ds)]
  (initialize-test-database! conn)
  (pipeline.integration-test/commit-log-batch! conn large-stress-batch))

(pipeline.integration-test/commit-log-batch! mock-ds large-stress-batch)

(defn read-ingestion-file [file-path]
  (with-open [reader (io/reader file-path)]
    (doall (csv/read-csv reader))))

(defn raw-row->spec-map [row]
  ;;Destructuring the array elements based on position index locations
  (let [[id-str status-str telemetry-str] row]
    {:pipeline/log_id (java.util.UUID/fromString (str/trim id-str))
     :pipeline/status (str/trim status-str)
     :pipeline/telemetry (Integer/parseInt (str/replace telemetry-str #"\s+" ""))}))

(raw-row->spec-map ["5552c77c-0792-4e81-a312-aa41c721a709","Dx23FP5UNM509X707iyP98S", "1"])

;;Mapping Full Ingest Loop
(defn process-csv-pipeline! [datasource file-path]
  ;; 1. To read all the rows from the physical disk file
  (let [raw-rows (read-ingestion-file file-path)
        ;; 2. For skipping the header row and map text rows into Spec maps
        clean-batch (map raw-row->spec-map (next raw-rows))]
    ;; 3. Pipe the clean batch maps straight into next.jdbc transaction!
    (pipeline.integration-test/commit-log-batch! datasource clean-batch)))

;; Real world data sample 
(defn create-troublesome-csv! [file-path]
  (let [bad-content "log_id,status,telemetry
5552c77c-0792-4e81-a312-aa41c721a709,active,850
ed816c31-d2fd-425a-8b4f-4581b249bf2b,processing,-15
c6ab1dbb-4f14-45c0-b6c4-1379155b0fb5,completed,3400  
"]
    (spit file-path bad-content)))

(create-troublesome-csv! "data/troublesome_telemetry.csv")

(process-csv-pipeline! mock-ds "data/troublesome_telemetry.csv")

(defn scan-and-list-csvs [dir-path]
  ;; 1. Converts the path string into a physical Java File Directory instance
 (let [directory (io/file dir-path)]
   ;;2. Filter  the file array, selecting  only items that end  with ".csv"
   (filter #(str/ends-with? (.getName %) ".csv")
           (.listFiles directory))))
(scan-and-list-csvs "data")

(defn ingest-directory-pipeline! [datasource dir-path]
  ;; 1. For scanning the target folder and gather  all physical CSV file objects
  (let [csv-files (scan-and-list-csvs dir-path)]
  ;; 2. The master loop over the discovered file array
    (doseq [file csv-files]
      (println "Starting Batch Ingestion for File:" (.getName file))
  ;; 3. Passing the absolute file path string directly to the pipeline engine
     (process-csv-pipeline! datasource (.getPath file)))))

(spit "data/clean_telemetry_batch.csv" "log_id,status,telemetrya7a6e8de-66b1-4596-b613-d8dc70352740,active,95006080148c-2d16-40ab-bff6-3e2c7026b7df,completed,-450")

(scan-and-list-csvs "data")

(ingest-directory-pipeline! mock-ds "data")

(defn archive-file! [file-object target-dir]
  ;; 1. Ensuring the physics-based archive folder exists on disk
  (.mkdirs (io/file target-dir))
  ;; 2. Construct the destination path location file object handle
  (let [dest-file (io/file target-dir (.getName file-object))]
    ;; 3. Atomically move the file across the hard drive sectors
    (.renameTo file-object dest-file)))
;;Testing filesystem mover capability by calling to telemetry file handle object
(archive-file! (io/file "data/clean_telemetry_batch.csv") "archive")

(defn ingest-directory-pipeline! [datasource dir-path]
  (let [csv-files (scan-and-list-csvs dir-path)]
    (doseq [file csv-files]
      (println "Starting Batch Ingestion for File:" (.getName file))
      (process-csv-pipeline! datasource (.getPath file))
      ;; INTEGRATION STEP: Securely archive the file resource upon successful commit
      (archive-file! file "archive")
      (println "File successfully committed and archived:" (.getName file)))))



