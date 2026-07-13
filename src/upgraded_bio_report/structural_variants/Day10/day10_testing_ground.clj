(ns day10-testing-ground
  (:require [clojure.string]))

(by-hand-dev) ;; Run this inside your Calva REPL to test the concept

(def line-transformer
  (comp
   (remove #(clojure.string/starts-with? % "#")) ; 1. Drop VCF headers
   (map clojure.string/trim)))                    ; 2. Clean whitespace

(defn line-transformer
  (comp
   (remove #(clojure.string/starts-with? % "#"))
   (map clojure.string/trim)))
  


