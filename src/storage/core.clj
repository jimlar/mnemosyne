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

(defn node-path 
  "
  Reads the stored nodes matching the hash, deepest node first.
  (all hash bits may not be used)
  "
  [db node-ptr hashes depth]
  (if (= 0 node-ptr)
    []
    (let [node (io/unmarshal-node db node-ptr)
          node-with-depth {:node node :depth depth}]
      (cond
        (io/leaf? node) [node-with-depth]
        (>= depth (count hashes)) (throw (java.lang.IllegalStateException. "need to implement hash collision handling"))
        :else
          (let [child-ptr ((:arcs node) (first hashes))]
            (if (nil? child-ptr) 
              [node-with-depth]
              (conj (node-path db child-ptr (rest hashes) (+ depth 1)) node-with-depth)))))))

(defn grow-branch [[leaf & branch] leaf-hashes insert-hashes]
  "Modify an existing branch for insertion of the insert hash key"
  (loop [depth (:depth leaf)
         branch branch]
    (if (= (nth leaf-hashes depth) (nth insert-hashes depth))
      (recur (+ 1 depth) (conj branch (io/node)))
      (conj branch (io/set-arc (io/node) (nth leaf-hashes depth) (:pos leaf))))))

(defn store
  "Store a key with a value, copying needed nodes, creating a new root and storing a new root pointer"
  ([key value] (store *db* key value))
  ([db key value]

    ; Find the node path for the hashed key,
    ; walk the node path and add new nodes for the modified branch
    ; - If the first node in path is a leaf, insert a new node 


    (let [hashes (hash-codes key)
          branch (node-path db (io/root-node db) hashes 0)
          first-node (:node (first branch))
          branch (if (io/leaf? first-node)
                    (grow-branch branch (hash-codes (:key first-node)) hashes)
                    branch)]

      ; Write the new leaf
      (io/write-bytes db (io/marshal-node (io/leaf key value) (io/end-pointer db)))

      ; Write the new branch
      (loop [node (first branch)
             nodes-left (rest branch)]
          (cond 
            (nil? (:node node)) db 
            :else
              (do
                (io/write-bytes db (io/marshal-node (io/set-arc (:node node) (hashes (:depth node)) (- (io/end-pointer) (io/node-size))) (io/end-pointer db)))
                (recur (first nodes-left) (rest nodes-left))))))

    (io/set-root-node db (- (io/end-pointer db) (io/node-size)))
    db))

(defn fetch 
  "Fetch a value for a key, returns nil if not found"
  ([key] (fetch *db* key))
  ([db key] 
    (let [node (:node (first (node-path db (io/root-node db) (hash-codes key) 0)))]
      (cond 
        (nil? node) nil
        (= (name key) (:key node)) (:value node)
        :else nil))))
