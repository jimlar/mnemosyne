(ns storage.core-test
  (:use midje.sweet)
  (:use storage.core))

(fact "store returns the stored data"
  (store {:a :b})
  => {:a :b})
