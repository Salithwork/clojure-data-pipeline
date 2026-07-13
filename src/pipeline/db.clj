(ns pipeline.db
  (:require [next.jdbc :as jdbc]
            [next.jdbc.sql :as sql]
            [clojure.string :as str]))

;; 1. Connection spec configuration map
(def db-spec
  {:dbtype "sqlite"
   :dbname "pipeline_storage.db"})

;; 2. Create the datasource instance
(def ds (jdbc/get-datasource db-spec))

;; 3. Database initialization table function
(defn initialize-database! []
  (jdbc/execute! ds ["
    CREATE TABLE IF NOT EXISTS processed_orders (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      order_id TEXT NOT NULL UNIQUE,
      customer_name TEXT,
      total_amount REAL,
      status TEXT,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );
  "]))

;; 4. Record insertion function
(defn save-order! [order-map]
  (sql/insert! ds :processed_orders
               {:order_id     (str/trim (:order-id order-map))
                :customer_name  (str/trim (get-in order-map [:customer_name]))
                :total_amount   (:total order-map)
                :status         "PROCESSED"}))
