(ns mnemosyne.hamt-test
  (:use midje.sweet)
  (:use mnemosyne.hamt)
  (:require [mnemosyne.io :as io]))
  
(defn mock-read-db [& data]
  (apply io/hexreader data))

(fact "leaf node are packed to marshalled key/value and zero bitmap"
  (io/hexdump (marshal-node (leaf "a" "b") 0))
  => "0000000161000000016200000000000000000000000000000000")

(fact "the offset is added to key/value back reference"
  (io/hexdump (marshal-node (leaf "a" "b") 4711))
  => "0000000161000000016200000000000012670000000000000000")

(fact "leaf node can be unmarshalled"
  (unmarshal-node (io/hexreader "0000000161" "0000000162" "0000000000000000" "0000000000000000") 10)
  => (leaf "a" "b" :pos 10))

(fact "arc-node is marshalled with arc pointer table"
  (io/hexdump (marshal-node (set-arc (node) 2 4711) 0))
  => "000000000000126700000000000000000000000000000004")

(fact "arc-node is marshalled with arc pointer table and file offset"
  (io/hexdump (marshal-node (set-arc (node) 2 4711) 3))
  => "000000000000126700000000000000030000000000000004")

(fact "arc-node is marshalled with arc pointer table"
  (unmarshal-node (io/hexreader "0000000000001267" "0000000000000000" "0000000000000004") 8)
  => (set-arc (node :pos 8) 2 4711))

(fact "node-path on empty db gives empty list"
  (node-path (io/hexreader "0000000000000000") 0 (hash-codes "a") 0)
  => [])

(fact "node-path on leaf db gives list with leaf"
  (node-path (io/hexreader "0000000000000012" "0000000161" "0000000162" "00000000000000080000000000000000") 18 (hash-codes "a") 0)
  => [(leaf "a" "b" :pos 18 :depth 0)])

(fact "node-path on arc-node with leaf db gives list with leaf and arc-node"
  (node-path (io/hexreader "000000000000002A" "0000000161" "0000000162" "00000000000000080000000000000000" "0000000000000012" "00000000000000220000000200000000") 42 (hash-codes "a") 0)
  => [(leaf "a" "b" :pos 18 :depth 1) (assoc (set-arc (node :pos 42) 33 18) :depth 0)])

(fact "fetch on leaf node only returns leaf value"
  (fetch
    (mock-read-db "0000000000000012" "0000000161" "0000000162" "00000000000000080000000000000000")
    "a")
  => "b")

(fact "store on empty db stores a leaf"
  (let [db (io/open-db)]
    (store db "a" "b")
    (io/hexdump db))
  => "00000000000000120000000161000000016200000000000000080000000000000000")

(fact "store single key value on empty db can be read back"
  (let [db (io/open-db)]
    (store db "the-key" "the value")
    (fetch db "the-key"))
  => "the value")

(fact "store single key value on empty db gives nil back for other key"
  (let [db (io/open-db)]
    (store db "the-key" "the value")
    (fetch db "the-other-key"))
  => nil)

(fact "store same key twice on empty db overwrites old value"
  (let [db (io/open-db)]
    (store db "the-key" "the value")
    (store db "the-key" "the other value")
    (fetch db "the-key"))
  => "the other value")

(fact "store two key/values on empty db can both be read back"
  (let [db (io/open-db)]
    (store db "the-key" "the value")
    (store db "the-other-key" "the other value")
    [(fetch db "the-key") (fetch db "the-other-key")])
  => ["the value" "the other value"])

(fact "grow branch should replace leaf with new node pointing to leaf"
  (grow-branch [(leaf "a" "b"  :pos 18 :depth 0)] [33 1 0 0 0 0 0 0 0 0] [34 1 0 0 0 0 0 0 0 0])
  => [(set-arc (node :depth 0) 33 18)])

(fact "grow branch should replace leaf with new nodes until hashes differ"
  (grow-branch [(leaf "a" "b" :pos 18 :depth 0)] [33 1 0 0 0 0 0 0 0 0] [33 2 0 0 0 0 0 0 0 0])
  => [(set-arc (node :depth 1) 1 18) (node :depth 0)]
  (grow-branch [(leaf "a" "b" :pos 18 :depth 0)] [33 1 0 0 0 0 0 0 0 0] [33 1 0 0 0 0 0 0 0 1])
  => [(set-arc (node :depth 9) 0 18)
      (node :depth 8)
      (node :depth 7)
      (node :depth 6)
      (node :depth 5)
      (node :depth 4)
      (node :depth 3)
      (node :depth 2)
      (node :depth 1)
      (node :depth 0)])

(fact "hash-code splits hash into 6 bit parts, lest significant first"
  (hash-codes "a")
  => [33 1 0 0 0 0 0 0 0 0]
  (hash-codes "the-key")
  => [3 12 3 28 47 62 63 63 63 63]
  (hash-codes "the-other-key")
  => [6 15 1 40 46 1 0 0 0 0])


(fact "100 keys can be stored and read back"
  (let [db (io/open-db)]
    (dorun (map #(store db (str "the-key-" %1) (str "the value " %1)) (range 100)))
    (vec (map #(fetch db (str "the-key-" %1)) (range 100))))
  => (vec (map #(str "the value " %1) (range 100))))
