(ns storage.core-test
  (:use midje.sweet)
  (:use storage.core))

(fact "store returns the stored data in a hash"
  (let [db (connect)]
    (store db :a "b")
    (fetch db :a))
  
  => "b")
