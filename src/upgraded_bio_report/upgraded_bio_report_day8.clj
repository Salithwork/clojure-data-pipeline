#!/usr/bin/env bb
(ns upgraded-bio-report.upgraded-bio-report-day8
  (:require [babashka.cli :as cli]
            [clojure.java.io :as io]
            [clojure.string :as str]))

;;1. Genetic Code Library
 (def codon-table
{"TTT" "F" "TTC" "F" "TTA" "L" "TTG" "L" "CTT" "L" "CTC" "L" "CTA" "L" "CTG" "L"
   "ATT" "I" "ATC" "I" "ATA" "I" "ATG" "M" "GTT" "V" "GTC" "V" "GTA" "V" "GTG" "V"
   "TCT" "S" "TCC" "S" "TCA" "S" "TCG" "S" "CCT" "P" "CCC" "P" "CCA" "P" "CCG" "P"
   "ACT" "T" "ACC" "T" "ACA" "T" "ACG" "T" "GCT" "A" "GCC" "A" "GCA" "A" "GCG" "A"
   "TAT" "Y" "TAC" "Y" "TAA" "*" "TAG" "*" "CAT" "H" "CAC" "H" "CAA" "Q" "CAG" "Q"
   "AAT" "N" "AAC" "N" "AAA" "K" "AAG" "K" "GAT" "D" "GAC" "D" "GAA" "E" "GAG" "E"
   "TGT" "C" "TGC" "C" "TGA" "*" "TGG" "W" "CGT" "R" "CGC" "R" "CGA" "R" "CGG" "R"
   "AGT" "S" "AGC" "S" "AGA" "R" "AGG" "R" "GGT" "G" "GGC" "G" "GGA" "G" "GGG" "G"})
  
;;2. Translation & Variation Utilities
(defn translate-dna [seq-str]
  (->> (partition 3 seq-str)
       (map #(str/join %))
       (map #(get codon-table % "?"))
       (str/join)))

(defn analyse-sequence [seq-str]
  (let [cleaned (str/upper-case (str/replace seq-str #"\s+" ""))
        len (count cleaned)
        counts (frequencies cleaned)
        n-count (get counts \N 0)
        n-pct (if (> len 0) (* (/ n-count len) 100.0) 0.0)
        g-count (get counts \G 0)
        c-count (get counts \C 0)
        gc-pct (if (> len 0) (* (/ (+ g-count c-count) len) 100.0) 0.0)
        protein (translate-dna cleaned)]
    
    {:length len
     :gc_pct gc-pct
     :n_pct n-pct
     :counts counts
     :protein (if (> (count protein) 50)
                (str (subs protein 0 50) "...[Truncated]")
                protein)}))

;;3 Stream Processor
(defn process-fasta [file-path]
  (with-open [reader (io/reader file-path)]
    (loop [lines (line-seq reader)
           current-seq []]
      (if-not (seq lines)
        (analyse-sequence (str/join current-seq))
        (let [line (str/trim (first lines))]
          (cond
            (str/blank? line) (recur (next lines) current-seq)
            (str/starts-with? line ">") (recur (next lines) current-seq)
            :else (recur (next lines) (conj current-seq line))))))))

;;4. Markdown Report Generator
(defn generate-markdown [data file-name]
  (let [{:keys [length gc_pct n_pct counts protein]} data
        passed? (< n_pct 5.0)] ; QC Threshold: Fail if >5% Ns
    (str "# Biological Health Report\n"
         "**Source File**: " file-name "\n"
         "**QC Status:** " (if passed? "✅ Passed " "❌Failed (High N count)") "\n\n"
         "# Sequence Metrics\n"
         "- **Total Base Pairs:** " length "\n"
         "- **GC Content:** " (format "%.2f" gc_pct) "%\n"
         "- **Ambiguous Bases (N):** " (format "%.2f" n_pct) "%\n\n"
         "## Nucleotide Breakdown\n\n"
         "| Base | Count |\n"
         "|------|-------|\n"
         "| Adenine (A)    | " (get counts \A 0) " |\n"
         "| Cytosine (C)   | " (get counts \C 0) " |\n"
         "| Guanine (G)    | " (get counts \G 0) " |\n"
         "| Thymine (T)    | " (get counts \T 0) " |\n"
         "| Unknown (N)    | " (get counts \N 0) " |\n\n"
         "# Translated Peptide Sequence (First 50 Amino Acids)\n\n"
         "`" protein "`\n")))
;; 5. Command Line Interface Specification
(def cli-spec
  {:spec {:input  {:alias :i :type :string :require true :desc "Path to input FASTA file"}
          :output {:alias :o :type :string :default "bio_health_report.md" :desc "Path to output Markdown report"}}})

;; 6. Main Entry Point   

(defn -main [& args]
  (try
    (let [opts (cli/parse-opts args cli-spec)]
      (if (:help opts)
        (println (cli/format-opts cli-spec))
        (let [input-path (:input opts)
              output-path (:output opts)]
          (if-not (.exists (io/file input-path))
            (println "Error Input file not found" input-path)
            (do
              (println "Processing file...")
              (let [metrics (process-fasta input-path)
                    markdown (generate-markdown metrics input-path)]
                (spit output-path markdown)
                (println "Success! Report saved to" output-path)))))))
    (catch Exception e
      (println "Error:" (.getMessage e))
      (println "\nUsage details")
      (println (cli/format-opts cli-spec)))))

                  
;; Execution anchor
(apply -main *command-line-args*)
