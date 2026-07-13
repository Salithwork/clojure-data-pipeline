(ns upgraded-bio-report.structural-variants.parser
  (:require [clojure.string as str]))

;;----PART 1: THE PATIENT GENOTYPE PARSER----
(defn parse-patient [format-str patient-str]
  (let [k-list (map keyword(str/split format-str #":")) v-list (str/split patient-str #":")]
  (zipmap k-list v-list)))
  
   (parse-patient "GT:DP" "0/1:45")
(defn line-transformer
  (comp
   (remove #(clojure.string/starts-with? % "#"))
   (map clojure.string/trim)))