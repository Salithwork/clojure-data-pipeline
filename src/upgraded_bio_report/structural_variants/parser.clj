(ns upgraded-bio-report.structural-variants.parser
  (:require [clojure.java.io :as io]
            [clojure.string :as str]))

;; --- SEC 1: LAZY TOKENIZATION ENGINE ---

(defn- parse-int
  [s]
  (try (Integer/parseInt s) (catch Exception _ nil)))

(defn- parse-info-column
  [info-str]
  (if (or (nil? info-str) (= info-str "."))
    {}
    (->> (str/split info-str #";")
         (map #(str/split % #"="))
         (filter #(= 2 (count %)))
         (into {} (map (fn [[k v]]
                         (let [kw (keyword k)]
                           [kw (if (contains? #{:SVLEN :END} kw)
                                 (parse-int v)
                                 v)])))))))

(defn tokenize-vcf-line
  [^String line]
  (let [cols (str/split line #"\t")]
    (when (>= (count cols) 8)
      {:chrom (nth cols 0)
       :pos   (parse-int (nth cols 1))
       :id    (nth cols 2)
       :ref   (nth cols 3)
       :alt   (nth cols 4)
       :info  (parse-info-column (nth cols 7))})))

(defn stream-structural-variants
  [file-path]
  (let [reader (io/reader file-path)]
    (->> (line-seq reader)
         (remove #(str/starts-with? % "#"))
         (map tokenize-vcf-line)
         (remove nil?))))

;; --- SEC 2: STREAM FILTERING LAYER ---

(defn filter-by-min-size
  [min-size variant-stream]
  (filter (fn [variant]
            (let [svlen (get-in variant [:info :SVLEN])]
              (if (number? svlen)
                (>= (Math/abs ^long svlen) min-size)
                true)))
          variant-stream))

;; --- SEC 3: AGGREGATION & REPORTING ---

(defn summarize-by-chrom
  [variants]
  (->> variants
       (group-by :chrom)
       (into {} (map (fn [[chrom records]]
                       [chrom {:count (count records)
                               :ids (map :id records)}])))))

 ;;(defn separator 
   ;;[info-str]
   ;;   (let [pairs (clojure.string/split info-str #";")
     ;;    chopped-pairs (map (fn [item] (clojure.string/split item #"=")) pairs)]
     ;;(into {} chopped-pairs)))


(separator "SVTYPE=DEL;SVLEN=-5000")

(defn separator
  [info-str]
  (let [pairs (clojure.string/split info-str #";")
        chopped-pairs (map (fn [item]
                             (let [[k v] (clojure.string/split item #"=")]
                               [(keyword k) v]))
                           pairs)]
      (into {} chopped-pairs)))

(separator "SVTYPE=DEL;SVLEN=-5000")
(defn line-transformer
  (comp
   (remove #(clojure.string/starts-with? % "#"))
   (map clojure.string/trim)))
