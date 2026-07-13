(ns scratchpad.scratchpad
  (:require [clojure.test :refer [is]] 
            [clojure.spec.alpha :as s] 
            [next.jdbc :as jdbc] 
            [clojure.spec.gen.alpha :as gen] 
            [next.jdbc.sql :as sql]))
  
;;1 The Core Infrastucture Map
(def pipeline-cfg {:port 8080 :timeout 3000 :engine "sqlite"})

;;2 The Verification Checks (Single-argument lookup syntax!)

(is (contains? pipeline-cfg :port))
(is (number? (:timeout pipeline-cfg)))
(is (string? (:engine pipeline-cfg)))

(is (contains? :port pipeline-cfg ))
(is (number? (pipeline-cfg :timeout )))

(is (string? (pipeline-cfg :engine )))

(is (contains? pipeline-cfg :port))
(is (number? (:timeout)))

(is (string? (:engine )))

(s/def :pipeline/id integer?)
(s/def :pipeline/status string?)
(s/valid? :pipeline/id 101 )
(s/valid? :pipeline/status 101)
(s/def :pipeline/log-entry (s/keys :req [:pipeline/id :pipeline/status]))
(s/valid? :pipeline/log-entry {:pipeline/id 200 :pipeline/status "active"})
(s/valid? :pipeline/log-entry {:pipeline/id 200 :pipeline/status "active"})
(s/valid? :pipeline/log-entry {:pipeline/id "Samuel" :pipeline/status "inactive"})
(s/explain :pipeline/log-entry {:pipeline/id "Samuel" :pipeline/status "inactive"})

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
(s/def :pipeline/unique-id uuid?)
(s/def :pipeline/status string?)
(s/def :pipeline/telemetry integer?)

;; 2. Combine all three components into the required key structure
(s/def :pipeline/log-entry (s/keys :req [:pipeline/unique-id :pipeline/status :pipeline/telemetry]))
(gen/sample (s/gen :pipeline/log-entry) 50)

(def mock-ds {:dbtype "sqlite" :dbname "memory"})
(pipeline.integration-test/commit-log-batch! mock-ds my-50-rows)

(defn initialize-test-database! [datasource]
  (jdbc/execute! datasource
                 ["CREATE TABLE telemetry_logs (id TEXT PRIMARY KEY, Status TEXT, telemetry INTEGER);"]))
(initialize-test-database! mock-ds)
(def my-update-row(clojure.spec.gen.alpha/sample (clojure.spec.alpha/gen :pipeline/log-entry) 50))
(pipeline.integration-test/commit-log-batch! mock-ds my-update-row)









