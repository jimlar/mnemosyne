(ns storage.core-test
  (:use midje.sweet)
  (:use storage.core)
  (:require [storage.io :as io]))
  
(defn mock-read-db [& data]
  (atom {:log nil :data (apply io/hexreader data)}))

(fact "fetch on leaf node only returns leaf value"
  (fetch
    (mock-read-db "0000000000000012" "0000000161" "0000000162" "00000000000000080000000000000000")
    :a)
  => "b")

(fact "store on empty db stores a leaf"
  (let [db (open-db)]
    (store db :a "b")
    (io/hexdump (:data @db)))
  => "00000000000000120000000161000000016200000000000000080000000000000000")

(fact "store single key value on empty db can be read back"
  (let [db (open-db)]
    (store db :the-key "the value")
    (fetch db :the-key))
  => "the value")

(fact "store single key value on empty db gives nil back for other key"
  (let [db (open-db)]
    (store db :the-key "the value")
    (fetch db :the-other-key))
  => nil)

(fact "store same key twice on empty db overwrites old value"
  (let [db (open-db)]
    (store db :the-key "the value")
    (store db :the-key "the other value")
    (fetch db :the-key))
  => "the other value")

#_(fact "store two key/values on empty db can both be read back"
  (let [db (open-db)]
    (store db :the-key "the value")
    (store db :the-other-key "the other value")
    [(fetch db :the-key) (fetch :the-other-key)])
  => ["the value" "the other value"])

