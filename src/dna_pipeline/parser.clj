(ns dna-pipeline.parser
  (:require [clojure.string :as str]))

(def line-transformer
  (comp (remove (fn [line] (str/starts-with? line "#")))
        (map str/trim)
        (remove str/blank?)
        (map (fn [line] (str/split line #"\s+")))))

(def mock-vcf-lines
  ["#CHROM\tPOS\tID\tREF\tALT"  ;; Should be dropped completely
   "chr1\t10177\t.\tA\tAC"      ;; Should be trimmed and split
   "   "                        ;; Will pass remove, but str/trim makes it empty. 
   "chr2\t20488\t.\tG\tGT"])

(ns dna-pipeline.parser
  (:require [clojure.string :as str]))


(def line-transformer
  (comp (remove (fn [line] (str/starts-with? line "#")))
        (map str/trim)
        (remove str/blank?)
        (map (fn [line] (str/split line #"\s+")))))

(def mock-vcf-lines
  ["#CHROM\tPOS\tID\tREF\tALT"
   "chr1\t10177\t.\tA\tAC"
   "   "
   "chr2\t20488\t.\tG\tGT"])

(transduce line-transformer conj mock-vcf-lines)

;; Senior level coding

(def line-transformer
  (comp
   (remove #(str/starts-with? % "#"))
   (map str/trim)
   (map #(str/split % #"\s+"))))


(def mock-vcf-lines
  ["#CHROM\tPOS\tID\tREF\tALT"
   "chr1\t10177\t.\tA\tAC"
   "   "
   "chr2\t20488\t.\tG\tGT"])

(transduce line-transformer conj mock-vcf-lines)

;; using zipmap to extract data into readable map or otherwords into proper assembly
(def headers [:chrom :pos :id :ref :alt])
(def row-data ["chr1" "10177" "." "A" "AC"])
(zipmap headers row-data)

(ns dna-pipeline.parser
  (:require [clojure.string :as str]))

;1 Defining target schema keys
(def vcf-header [:chromosome :position :id :reference :alteration])

(def structural-parser
  (comp
   (remove #(str/starts-with? % "#"))
   (map str/trim)
   (remove str/blank?)                         ; For dropping empty rows
   (map #(str/split % #"\s+"))
   (map #(zipmap vcf-header %))))              ; Task : Zipping keys to data elements

(def mock-vcf-lines
  ["#CHROM\tPOS\tID\tREF\tALT"
   "chr1\t10177\t.\tA\tAC"
   "   "
   "chr2\t20488\t.\tG\tGT"])

(ns dna-pipeline.parser
  (:require [clojure.string :as str]))

(def vcf-header [:chromosome :position :id :reference :alteration])

(def structural-parser
  (comp
   (remove #(str/starts-with? % "#"))
   (map str/trim)
   (remove str/blank?)                          ; For dropping empty rows
   (map #(str/split % #"\s+"))
   (filter #(= (count %) (count vcf-header)))    ; Structural sheild to avoid the any anomalies like no header absent column present and viceversa
   (map #(zipmap vcf-header %))))

(def mock-vcf-lines
  ["#CHROM\tPOS\tID\tREF\tALT"
   "chr1\t10177\t.\tA\tAC"
   "   "
   "chr2\t20488\t.\tG"                        ;; Should be dropped automatically!
   "chr3\t30551\t.\tT\tTC"]) ; Task : Zipping keys to data elements

(transduce structural-parser conj mock-vcf-lines)

(ns dna-pipeline.parser
  (:require [clojure.string :as str]))

(def vcf-header [:chromosome :position :id :reference :alteration])

(def auditing-parser
  (comp
   (remove #(str/starts-with? % "#"))
   (map str/trim)
   (remove str/blank?)
   (map #(str/split % #"\s+"))
   (map (fn [tokens]
          (if (= (count tokens) (count vcf-header))
            {:type :ok  :payload (zipmap vcf-header tokens)}
            {:type :error :payload tokens})))))

(def mock-vcf-lines
  ["#CHROM\tPOS\tID\tREF\tALT"
   "chr1\t10177\t.\tA\tAC"
   "   "
   "chr2\t20488\t.\tG"
   "chr3\t30551\t.\tT\tTC"])


(transduce auditing-parser conj mock-vcf-lines)

(ns dna-pipeline.parser
  (:require [clojure.string :as str]))

(def vcf-header [:chromosome :position :id :reference :alteration])

(def auditing-parser
  (comp
   (remove #(str/starts-with? % "#"))
   (map str/trim)
   (remove str/blank?)
   (map #(str/split % #"\s+"))
   (map (fn [tokens]
         (if (= (count tokens) (count vcf-header))
           {:type :ok :payload (zipmap vcf-header tokens)}
           {:type :error :payload tokens})))))

(def mock-vcf-lines
  ["#CHROM\tPOS\tID\tREF\tALT"
   "chr1\t10177\t.\tA\tAC"
   "   "
   "chr2\t20488\t.\tG"
   "chr3\t30551\t.\tT\tTC"])

(transduce auditing-parser conj mock-vcf-lines)


