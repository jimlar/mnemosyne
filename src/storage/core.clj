(ns storage.core
  (:require [storage.io :as io]))

(def ^:private ^:dynamic *db* nil)

(defmacro with-open-db 
  "Evaluate body with db as the current database and close db after"
  [db & body] 
  `(binding [*db* ~db] ~@body (close-db ~db)))

(defn open-db
  ([] (open-db (str (io/temp-file))))
  ([file] (io/open-file file)))

(defn close-db [db]
  (io/close db))

(defn hash-codes
  "Hash a string key, returns a list of the 6-bit parts of the hash code"
  [key]
  (let [code (hash key)]
    (map #(bit-and (bit-shift-right code (* 6 %1)) 63) (range 0 10))))

(defn hash-code 
  [key]
  (hash key))

(defn child-index [hash-bits]
  (bit-and hash-bits 63))

(defn next-hash-part [hash-bits]
  (bit-shift-right hash-bits 6))

(defn node-path 
  "
  Reads the stored nodes matching the hash, deepest node first.
  (all hash bits may not be used)
  "
  [db node-ptr hash-bits index-in-parent]
  (if (= 0 node-ptr)
    []
    (let [node (io/unmarshal-node db node-ptr)
          node-with-index {:node node :index index-in-parent}]
      (if (io/leaf? node) 
        [node-with-index]
        (let [child-ptr ((:arcs node) (child-index hash-bits))]
          (if (nil? child-ptr) 
            [node-with-index]
            (conj (node-path db child-ptr (next-hash-part hash-bits) (child-index hash-bits)) node-with-index)))))))

(defn add-to-branch 
  "
  Adds a key/value to an existing branch, the branch is typically the result of calling node-path
  "
  [key value hash branch]
  )

(defn store 
  "Store a key with a value, copying needed nodes, creating a new root and storing a new root pointer"
  ([key value] (store *db* key value))
  ([db key value] 

    ; Find the node path for the hashed key,
    ; walk the node path and add new nodes for the modified branch
    ; - If the first node in path is a leaf, insert a new node 

    (io/write-bytes db (io/marshal-node (io/leaf key value) (io/end-pointer db)))



    (io/set-root-node db (- (io/end-pointer db) (io/node-size)))
    db))

(defn fetch 
  "Fetch a value for a key, returns nil if not found"
  ([key] (fetch *db* key))
  ([db key] 
    (let [node (:node (first (node-path db (io/root-node db) (hash-code key) -1)))]
      (cond 
        (nil? node) nil
        (= (name key) (:key node)) (:value node)
        :else nil))))
