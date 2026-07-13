#!/usr/bin/env bb
(ns bio-report
  (:require [clojure.java.io :as io]
            [clojure.string :as str]))
;;1 Core Analytical Engine
(defn analyze-sequence [seq-str]
  (let [cleaned (str/upper-case (str/replace seq-str #"\s+" ""))
        len (count cleaned)
        counts (frequencies cleaned)
        g-count (get counts \G 0)
        c-count (get counts \C 0)
        gc-pct (if (> len 0)
                 (* (/ (+ g-count c-count) len) 100.0)
                 0.0)]
    {:length len
     :gc-pct gc-pct
     :counts counts}))
;;2 Stream  Processor for FASTA files
(defn process-fasta [file-path]
  ;; Simple FASTA processor: concatenate all sequence lines (ignore headers starting with '>')
  (with-open [reader (io/reader file-path)]
    (let [lines (line-seq reader)
          seq-lines (->> lines
                         (remove #(str/blank? %))
                         (remove #(str/starts-with? (str/trim %) ">")))]
      (analyze-sequence (apply str (map str/trim seq-lines))))))
  ;;Markdown Generator
  (defn markdown-report [data file-name]
    (let [{:keys [length gc-pct counts]} data]
      (str "# Biological Health Report for " file-name "\n\n"
           "**Source File:** " file-name "\n\n"
           "## Sequence Metrics\n"
           "- **Total Base Pairs:** " length "\n"
           "- **GC Content:** " (format "%.2f" gc-pct) "%\n\n"
           "## Nucleotide Breakdown\n"
           "|Base | Count |\n"
           "|-----|-------|\n"
           "|Adenine (A)    | " (get counts \A 0) " |\n"
           "|Thymine (T)    | " (get counts \T 0) " |\n"
           "|Guanine (G)    | " (get counts \G 0) " |\n"
           "|Cytosine (C)   | " (get counts \C 0) " |\n")))

      ;;CLI Entry Point
  (defn -main [& args]
    (let [file-path (first args)]
      (if-not file-path
        (println "Error: Please provide a path to a FASTA file. Usage: bb bio_report.clj <path-to-fasta-file>")
        (if-not (.exists (io/file file-path))
          (println "Error: File not found at" file-path)
          (let [metrics (process-fasta file-path)
                markdown (markdown-report metrics file-path)]
            (spit "bio_health_report.md" markdown)
            (println "Success! Report generated at 'bio_health_report.md'"))))))
;; Enable execution when invoked via command line 
(apply -main *command-line-args*)