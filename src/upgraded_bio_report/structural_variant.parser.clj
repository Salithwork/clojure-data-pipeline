(ns structural-variant.parser
  (:require [clojure.java.io :as io]
            [clojure.string :as str]))

(defn metadata-line?
  "Returns true if the line is a VCF metadata or column header line."
  [^String line]
  (str/starts-with? line "#"))

(defn stream-vcf-variants
  "Lazily streams variant data lines from a VCF file, skipping all headers."
  [file-path]
  (let [reader (io/reader file-path)]
    (->> (line-seq reader)
         (remove metadata-line?))))

(defn generate-mock-vcf!
  "Programmatically generates the structural variants mock data file."
  []
  (let [dir (io/file "F:/clojure/data")
        path "F:/clojure/data/structural_variants.vcf"
        content (str "##fileformat=VCFv4.2\n"
                     "##ALT=<ID=DEL,Description=\"Deletion\">\n"
                     "##ALT=<ID=DUP,Description=\"Duplication\">\n"
                     "##ALT=<ID=INV,Description=\"Inversion\">\n"
                     "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n"
                     "chr1\t10000\tsv1\tN\t<DEL>\t60\tPASS\tSVTYPE=DEL;SVLEN=-5000;END=15000\n"
                     "chr2\t20000\tsv2\tN\t<DUP>\t60\tPASS\tSVTYPE=DUP;SVLEN=10000;END=30000\n"
                     "chr3\t30000\tsv3\tN\t<INV>\t60\tPASS\tSVTYPE=INV;SVLEN=0;END=35000\n")]
    (.mkdirs dir)
    (spit path content)
    (println "VCF successfully generated at:" path)))

