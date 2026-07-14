(ns scratchpad.scratchpad
  (:require [clojure.test :refer [is]]
            [clojure.spec.alpha :as s]
            [next.jdbc :as jdbc]
            [clojure.spec.gen.alpha :as gen]
            [next.jdbc.sql :as sql]
            [clojure.data.csv :as csv]
            [clojure.java.io :as io]))

(defn insert-to-sqlite! [data] :stored-successfully)

(defn save-pipeline-log! [payload]
  (if (s/valid? :pipeline/log-entry payload)
    (insert-to-sqlite! payload)
    (throw (ex-info "Pipeline Write Intercepted: Payload failed schema validation contract."
                    {:explain-data (s/explain-data :pipeline/log-entry payload)}))))

(save-pipeline-log! {:pipeline/id 305 :pipeline/status "processing"})
(save-pipeline-log! {:pipeline/id "samuel" :pipeline/status "processing"})

(let [batch-box [{:pipeline/id 101 :pipeline/status "active"}
                 {:pipeline/id "Samuel" :pipeline/status "error"}]
      (doseq (println "Processing item ID:" (:pipeline/id payload)))])

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

;; 1. Re-evaluate your base rules into the empty JVM memory
(s/def :pipeline/id integer?)
(s/def :pipeline/status string?)

;; 2. Re-evaluate your composite map blueprint
(s/def :pipeline/log-entry (s/keys :req [:pipeline/id :pipeline/status]))

;; 3. NOW run your generator factory!
(gen/sample (s/gen :pipeline/log-entry) 3)

;; 1. Register your individual data component types
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
    {:pipeline/log_id (java.util.UUID/fromString id-str)
     :pipeline/status status-str
     :pipeline/telemetry (Integer/parseInt telemetry-str)}))

(raw-row->spec-map ["5552c77c-0792-4e81-a312-aa41c721a709","Dx23FP5UNM509X707iyP98S", "1"])

;;Mapping Full Ingest Loop
(defn process-csv-pipeline! [datasource file-path]
  ;; 1. To read all the rows from the physical disk file
  (let [row-raws (read-ingestion-file file-path)
    ;; 2. For skipping the header row and map text rows into Spec maps
        clean-batch (map raw-row->spec-map (next raw-rows))]
    ;; 3. Pipe the clean batch maps straight into next.jdbc transaction!
    (pipeline.integration-test/commit-log-batch! datasource clean-batch)))