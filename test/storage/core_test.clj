(ns storage.core-test
  (:use midje.sweet)
  (:use storage.core))
  
(fact "fetch returns stored value"
  (with-open-db (open-db)
    (store :a "b")
    (fetch :a))
  => "b")

(fact "store on one connection affects fetch on other"
  (with-open-db (open-db)
    (store :a "b"))
  (with-open-db (open-db)
    (fetch :a))
  => "b")
