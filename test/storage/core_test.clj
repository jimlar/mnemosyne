(ns storage.core-test
  (:use midje.sweet)
  (:use storage.core))
  
(fact "fetch returns stored value"
  (with-db (start)
    (store :a "b")
    (fetch :a))
  => "b")

(fact "store on one connection affects fetch on other"
  (with-db (start)
    (store :a "b"))
  (with-db (start)
    (fetch :a))
  => "b")
