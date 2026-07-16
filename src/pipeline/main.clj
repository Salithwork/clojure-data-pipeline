(ns pipeline.main
  (:require [scratchpad.scratchpad :as pipeline])
  (:gen-class)) ;;For Compiling  down a standard  Java Executable class file

1. For checking if the terminal operator  provided a directory  path argument
(defn -main [& args]
  (if-let [target-dir (first args)]
    (do (println "Initializing  CLI Data Pipeline Engine ...")
;; For executing  the fully automated directory ingest routine cleanly
    (pipeline/ingest-directory-pipeline! pipeline/mock-ds target-dir)
    (println "Directory Ingestion Pipeline Complete"))
    (println "ERROR: Missing target directory path. Usage: clj -M -m pipeline.main <directory_path>")))
;;Entry  point function, passing "data" as a  mock terminal argument
(pipeline.main/-main "data")
