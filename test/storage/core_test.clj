(ns storage.core-test
  (:use midje.sweet)
  (:use storage.core))

(fact "fetch returns stored value"
  (let [db (connect)]
    (store db :a "b")
    (fetch db :a))
  => "b")
