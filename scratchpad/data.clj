(ns scratchpad.data
  (:require [clojure.set :as set])
  (:require [clojure.set :refer [difference]]))

(set/intersection #{1 2} #{2 3})

(difference #{1 2} #{2 3})