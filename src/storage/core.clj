(ns storage.core
  (:require [storage.io :as io]))

(def ^:private ^:dynamic *db* nil)

(defmacro with-open-db 
  "Evaluate body with db as the current database and close db after"
  [db & body] 
  `(binding [*db* ~db] ~@body (close-db ~db)))

(defn open-db
  ([] (open-db (str (io/temp-dir))))
  ([dir] (agent (io/open-dir dir))))

(defn close-db [db]
  (send-off db io/close)
  (await db)
  db)

(defn hash-code 
  "Hash a string key"
  [key]
  (hash (name key)))

(defn child-index [hash-bits]
  (bit-and hash-bits 63))

(defn next-hash-part [hash-bits]
  (bit-shift-right hash-bits 6))

; Create a list of hash parts instead?

(defn node-path 
  "
  Reads the stored nodes matching the hash, deepest node first.
  (all hash bits may not be used)
  "
  [in node-ptr hash-bits index-in-parent]
  (if (= 0 node-ptr)
    []
    (let [node (io/unmarshal-node in node-ptr)
          node-with-index {:node node :index index-in-parent}]
      (if (io/leaf? node) 
        [node-with-index]
        (let [child-ptr ((:arcs node) (child-index hash-bits))]
          (if (nil? child-ptr) 
            [node-with-index]
            (conj (node-path in child-ptr (next-hash-part hash-bits) (child-index hash-bits)) node-with-index)))))))



(defn store 
  "Store a key with a value, copying needed nodes, creating a new root and storing a new root pointer"
  ([key value] (store *db* key value))
  ([db key value] 
    (send-off 
      db
      (fn [db]

        ; Find the node path for the hashed key,
        ; walk the node path and add new nodes for the modified branch
        ; - If the first node in path is a leaf, insert a new node 

        (io/write-bytes db (io/marshal-node (io/leaf key value) (io/end-pointer db)))



        (io/set-root-node db (- (io/end-pointer db) (io/node-size)))))
    (await db)
    db))

(defn fetch 
  "Fetch a value for a key, returns nil if not found"
  ([key] (fetch *db* key))
  ([db key] 
    (let [in (:data @db)
          node (:node (first (node-path in (io/root-node in) (hash-code key) -1)))]
      (cond 
        (nil? node) nil
        (= (name key) (:key node)) (:value node)
        :else nil))))
