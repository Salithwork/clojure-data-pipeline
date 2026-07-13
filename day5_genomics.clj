(ns day5-genomics)

;; 1. Define a mock DNA sequence (exons/introns)
(def dna-strand "ATGAAGCCGTTTTCGAATCGAGCT")

;; 2. Calculate the GC-Content (Crucial for DNA stability/melting points)
(defn calculate-gc-content [sequence]
  (let [total-length (count sequence)
        nucleotide-counts (frequencies sequence)
        g-count (get nucleotide-counts \G 0)
        c-count (get nucleotide-counts \C 0)
        gc-total (+ g-count c-count)]
    (double (* (/ gc-total total-length) 100))))
(calculate-gc-content dna-strand)
(defn calculate-gc-content [sequence]
  (let [clean-seq (clojure.string/upper-case sequence)
        total-length (count clean-seq)]
    (if (zero? total-length)
      0.0
      (let [nucleotide-counts (frequencies clean-seq)
            g-count (get nucleotide-counts \G 0)
            c-count (get nucleotide-counts \C 0)
            gc-total (+ g-count c-count)]
        ;; Cast to double during division to avoid unnecessary Ratio types
        (* (/ (double gc-total) total-length) 100)))))

;; 3. Run your biological analysis pipeline
(calculate-gc-content dna-strand)
;; 4. Transcribe DNA strand to RNA strand
(defn transcribe-dna-to-rna [sequence]
  (let [transcribe-char (fn [nucleotide]
                          (if (= nucleotide \T) \U nucleotide))]
    (apply str (map transcribe-char sequence))))
(transcribe-dna-to-rna dna-strand)

;; 6. A list of patient DNA sample strings
(def patient-samples ["ATGAAGCCG" 
                       "ATGXAGCCG" 
                       "ATGAAGCCU" 
                       "ATGAXGCCG"])

;; 7. Predicate function to check if a sample is clean (No mutated 'X')
(defn clean-sample? [sequence]
  (not (clojure.string/includes? sequence "X")))

;; 8. Filter out the mutated samples
(filter clean-sample? patient-samples)



