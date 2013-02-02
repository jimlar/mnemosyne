(ns storage.core-test
  (:use midje.sweet)
  (:use storage.core)
  (:require [storage.io :as io]))
  
(defn mock-read-db [& data]
  (apply io/hexreader data))

(fact "node-path on empty db gives empty list"
  (node-path (io/hexreader "0000000000000000") 0 (hash-codes "a") 0)
  => [])

(fact "node-path on leaf db gives list with leaf"
  (node-path (io/hexreader "0000000000000012" "0000000161" "0000000162" "00000000000000080000000000000000") 18 (hash-codes "a") 0)
  => [{:node (io/leaf 18 "a" "b") :depth 0}])

(fact "node-path on arc-node with leaf db gives list with leaf and arc-node"
  (node-path (io/hexreader "000000000000002A" "0000000161" "0000000162" "00000000000000080000000000000000" "0000000000000012" "00000000000000220000000200000000") 42 (hash-codes "a") 0)
  => [{:node (io/leaf 18 "a" "b") :depth 1} {:node (io/set-arc (io/node 42) 33 18) :depth 0}])

(fact "fetch on leaf node only returns leaf value"
  (fetch
    (mock-read-db "0000000000000012" "0000000161" "0000000162" "00000000000000080000000000000000")
    :a)
  => "b")

(fact "store on empty db stores a leaf"
  (let [db (open-db)]
    (store db :a "b")
    (io/hexdump db))
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

(fact "store two key/values on empty db can both be read back"
  (let [db (open-db)]
    (store db :the-key "the value")
    (store db :the-other-key "the other value")
    [(fetch db :the-key) (fetch db :the-other-key)])
  => ["the value" "the other value"])

(fact "hash-code splits hash into 6 bit parts, lest significant first"
  (hash-codes "a")
  => [33, 1, 0, 0, 0, 0, 0, 0, 0, 0])