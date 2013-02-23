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
  => [(io/leaf 18 "a" "b" 0)])

(fact "node-path on arc-node with leaf db gives list with leaf and arc-node"
  (node-path (io/hexreader "000000000000002A" "0000000161" "0000000162" "00000000000000080000000000000000" "0000000000000012" "00000000000000220000000200000000") 42 (hash-codes "a") 0)
  => [(io/leaf 18 "a" "b" 1) (assoc (io/set-arc (io/node :pos 42) 33 18) :depth 0)])

(fact "fetch on leaf node only returns leaf value"
  (fetch
    (mock-read-db "0000000000000012" "0000000161" "0000000162" "00000000000000080000000000000000")
    "a")
  => "b")

(fact "store on empty db stores a leaf"
  (let [db (open-db)]
    (store db "a" "b")
    (io/hexdump db))
  => "00000000000000120000000161000000016200000000000000080000000000000000")

(fact "store single key value on empty db can be read back"
  (let [db (open-db)]
    (store db "the-key" "the value")
    (fetch db "the-key"))
  => "the value")

(fact "store single key value on empty db gives nil back for other key"
  (let [db (open-db)]
    (store db "the-key" "the value")
    (fetch db "the-other-key"))
  => nil)

(fact "store same key twice on empty db overwrites old value"
  (let [db (open-db)]
    (store db "the-key" "the value")
    (store db "the-key" "the other value")
    (fetch db "the-key"))
  => "the other value")

(fact "store two key/values on empty db can both be read back"
  (let [db (open-db)]
    (store db "the-key" "the value")
    (store db "the-other-key" "the other value")
    [(fetch db "the-key") (fetch db "the-other-key")])
  => ["the value" "the other value"])

(fact "grow branch should replace leaf with new node pointing to leaf"
  (grow-branch [(io/leaf 18 "a" "b" 0)] [33 1 0 0 0 0 0 0 0 0] [34 1 0 0 0 0 0 0 0 0])
  => [(io/set-arc (io/node :depth 0) 33 18)])

(fact "grow branch should replace leaf with new nodes until hashes differ"
  (grow-branch [(io/leaf 18 "a" "b" 0)] [33 1 0 0 0 0 0 0 0 0] [33 2 0 0 0 0 0 0 0 0])
  => [(io/set-arc (io/node :depth 1) 1 18) (io/node :depth 0)]
  (grow-branch [(io/leaf 18 "a" "b" 0)] [33 1 0 0 0 0 0 0 0 0] [33 1 0 0 0 0 0 0 0 1])
  => [(io/set-arc (io/node :depth 9) 0 18)
      (io/node :depth 8)
      (io/node :depth 7)
      (io/node :depth 6)
      (io/node :depth 5)
      (io/node :depth 4)
      (io/node :depth 3)
      (io/node :depth 2)
      (io/node :depth 1)
      (io/node :depth 0)])

(fact "hash-code splits hash into 6 bit parts, lest significant first"
  (hash-codes "a")
  => [33 1 0 0 0 0 0 0 0 0]
  (hash-codes "the-key")
  => [3 12 3 28 47 62 63 63 63 63]
  (hash-codes "the-other-key")
  => [6 15 1 40 46 1 0 0 0 0])


(fact "100 keys can be stored and read back"
  (let [db (open-db)]
    (dorun (map #(store db (str "the-key-" %1) (str "the value " %1)) (range 100)))
    (vec (map #(fetch db (str "the-key-" %1)) (range 100))))
  => (vec (map #(str "the value " %1) (range 100))))
