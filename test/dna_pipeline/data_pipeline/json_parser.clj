(ns dna-pipeline.data-pipeline.json-parser
  (:require[clojure.java.io :as io]
           [cheshire.core :as json]))

(defn process-anomaly-payload [file-path]
  (let [target-file (io/file file-path)]
    (if (.exists target-file)
      (with-open [rdr (io/reader target-file)]
        (json/parse-stream rdr true)) nil)))

(process-anomaly-payload "F:/clojure/target/permanent_error_log.json")


