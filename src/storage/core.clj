(ns storage.core
  (:import [clojure pprint__init])
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
  (let [code (hash (name key))]
    (map #(bit-and (bit-shift-right code (* 6 %1)) 63) (range 0 10))))

(defn node-path 
  "
  Reads the stored nodes matching the hash, deepest node first.
  (all hash bits may not be used)
  "
  [db node-ptr hashes depth]
  (if (= 0 node-ptr)
    []
    (let [node (io/unmarshal-node db node-ptr depth)]
      (cond
        (io/leaf? node) [node]
        (>= depth (count hashes)) (throw (java.lang.IllegalStateException. "need to implement hash collision handling"))
        :else
          (let [child-ptr ((:arcs node) (first hashes))]
            (if (nil? child-ptr) 
              [node]
              (conj (node-path db child-ptr (rest hashes) (+ depth 1)) node)))))))

(defn grow-branch [[leaf & branch] leaf-hashes insert-hashes]
  "Modify an existing branch for insertion of the insert hash key"
  (loop [depth (:depth leaf)
         branch branch]
    (if (= (nth leaf-hashes depth) (nth insert-hashes depth))
      (recur (+ 1 depth) (conj branch (io/node :depth depth)))
      (conj
        branch
        (let [new-node (io/set-arc (io/node :depth depth) (nth leaf-hashes depth) (:pos leaf))]
          #_(print (str "Modified node, arcno " (nth leaf-hashes depth) ", pos " (:pos leaf) ", leaf-hashes " (vec leaf-hashes) ", node: " new-node "\n"))
          new-node)))))

(defn store
  "Store a key with a value, copying needed nodes, creating a new root and storing a new root pointer"
  ([key value] (store *db* key value))
  ([db key value]

    ; Find the node path for the hashed key,
    ; walk the node path and add new nodes for the modified branch

    (let [hashes (hash-codes key)
          branch (node-path db (io/root-node db) hashes 0)
          first-node (first branch)
          branch (if (io/leaf? first-node)
                    (if (= key (:key first-node))
                      (rest branch)
                      (grow-branch branch (hash-codes (:key first-node)) hashes))
                    branch)
          new-leaf (io/leaf key value)]

      ; Write the new leaf
      #_(print (str "Writing new leaf to " (io/end-pointer db) ", hashes " (vec hashes) ", " new-leaf "\n"))
      (io/write-bytes db (io/marshal-node new-leaf (io/end-pointer db)))

      ; Write the grown branch, deepest node first, modifying the arc pointers for the branch processed
      (dorun
        (for [node branch]
          (io/write-bytes
            db
            (io/marshal-node
              (let [node (io/set-arc node (nth hashes (:depth node)) (- (io/end-pointer db) (io/node-size)))]
                #_(print (str "Writing key " key ", hash " (vec hashes) ", node " node "\n"))
                node)
              (io/end-pointer db))))))

    (io/set-root-node db (- (io/end-pointer db) (io/node-size)))
    db))

(defn fetch 
  "Fetch a value for a key, returns nil if not found"
  ([key] (fetch *db* key))
  ([db key]
    (let [hash (hash-codes key)
          path (node-path db (io/root-node db) hash 0)
          node (first path)]
      #_(print (str "Fetching " key ", hash " (vec hash) ", path " path "\n"))
      (cond 
        (nil? node) nil
        (= (name key) (:key node)) (:value node)
        :else nil))))
