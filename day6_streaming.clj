(ns day6-streaming
  (:require [clojure.java.io :as io]
            [clojure.string :as str]))

(def filepath "mock_genome.fasta")

;; ZONE A: The Function Definition Block
(defn stream-gc-counter [filepath]
  (with-open [rdr (io/reader filepath)]
    (let [lines (line-seq rdr)
          clean-lines (filter (fn [line] (not (str/starts-with? line ">"))) lines)
          nucleotide-map (frequencies (apply str clean-lines))
          g-count (get nucleotide-map \G 0)
          c-count (get nucleotide-map \C 0)
          total-bases (reduce + (vals nucleotide-map))]
      {:gc-percent (double (* (/ (+ g-count c-count) total-bases) 100))
       :total-bases total-bases})))

;; ZONE B: The Execution Line
(stream-gc-counter filepath)
