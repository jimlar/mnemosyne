(ns mnemosyne.hamt
  (:require [mnemosyne.io :as io]))

;;;;;;;;;;;;;;;;;; hash related fns ;;;;;;;;;;;;;;;;;;

(defn hash-codes
  "Hash a string key, returns a list of the 6-bit parts of the hash code"
  [^String key]
  (let [code (hash key)]
    (map #(bit-and (bit-shift-right code (* 6 %1)) 63) (range 0 10))))

;;;;;;;;;;;;;;;;;; nodes ;;;;;;;;;;;;;;;;;;

(defn node-size [] 16)

(defn node
  "Create a HAMT node with optional position, arcbits and arc pointers"
  [& {:keys [pos arcbits arcs depth] :or {pos nil arcbits 0 arcs (repeat 64 nil) depth nil}}]
  {:pos pos :arcbits arcbits :arcs (vec arcs) :depth depth})

(defn set-arc
  "Change one arc of a node"
  [node arc-no arc-ptr]
  (assoc
    node
    :arcbits (long (bit-set (:arcbits node) arc-no))
    :arcs (assoc (:arcs node) arc-no arc-ptr)))

(defn leaf
  "A HAMT leaf node, with a key and a value"
  [key value & node-params] (assoc (apply node node-params) :key key :value value))

(defn leaf?
  "Return true if node is a leaf"
  [node]
  (= 0 (:arcbits node)))

;;;;;;;;;;;;;;;;;; node marshaling ;;;;;;;;;;;;;;;;;;

(defn marshal-node
  "Create bytes from node, offset is added to all pointers"
  [node offset]
  (if (leaf? node)
    (byte-array (concat (io/marshal-string (name (:key node)))
                  (io/marshal-string (:value node))
                  (io/marshal-long offset)
                  (io/marshal-long 0)))
    (byte-array (concat (apply concat (map io/marshal-long (filter #(not (nil? %)) (:arcs node))))
                  (io/marshal-long offset)
                  (io/marshal-long (:arcbits node))))))

(defn unmarshal-arc-table
  "Read arc pointer table into arc vector"
  [arcbits db pos]
  (reduce
    (fn [v bit]
      (if (bit-test arcbits bit)
        (conj v (io/unmarshal-long db (+ pos (* (count (filter (complement nil?) v)) 8))))
        (conj v nil)))
    []
    (range 64)))

(defn unmarshal-node
  "Read node from bytes"
  ([db position depth]
    (assoc (unmarshal-node db position) :depth depth))
  ([db position]
    (let [pointer (io/unmarshal-long db position)
          arcbits (io/unmarshal-long db (+ 8 position))]
      (if (= 0 arcbits) ; leaf node or arc node?
        (let [key (io/unmarshal-string db pointer)
              pointer (+ pointer 4 (count key))
              value (io/unmarshal-string db pointer)]
          (leaf key value :pos position))
        (node :pos position :arcbits arcbits :arcs (unmarshal-arc-table arcbits db pointer))))))

;;;;;;;;;;;;;;;;;; hamt insert ;;;;;;;;;;;;;;;;;;

(defn node-path
  "
  Reads the stored nodes matching the hash, deepest node first.
  (all hash bits may not be used)
  "
  [db node-ptr hashes depth]
  (if (= 0 node-ptr)
    []
    (let [node (unmarshal-node db node-ptr depth)]
      (cond
        (leaf? node) [node]
        (>= depth (count hashes)) (throw (java.lang.IllegalStateException. "need to implement hash collision handling"))
        :else
          (if-let [child-ptr ((:arcs node) (first hashes))]
            (conj (node-path db child-ptr (rest hashes) (+ depth 1)) node)
            [node])))))

(defn grow-branch [[leaf & branch] leaf-hashes insert-hashes]
  "Modify an existing branch for insertion of the insert hash key"
  (loop [depth (:depth leaf)
         branch branch]
    (if (= (nth leaf-hashes depth) (nth insert-hashes depth))
      (recur (+ 1 depth) (conj branch (node :depth depth)))
      (conj
        branch
        (set-arc (node :depth depth) (nth leaf-hashes depth) (:pos leaf))))))

(defn store
  "Store a key with a value, copying needed nodes, creating a new root and storing a new root pointer"
  [db key value]
    (let [hashes (hash-codes key)
          branch (node-path db (io/root-node-ptr db) hashes 0)
          first-node (first branch)
          branch (if (leaf? first-node)
                    (if (= key (:key first-node))
                      (rest branch)
                      (grow-branch branch (hash-codes (:key first-node)) hashes))
                    branch)

          ; Write the new leaf
          file-ptr (io/end-pointer db)
          file-ptr (io/write-bytes db (marshal-node (leaf key value) file-ptr) file-ptr)]

      ; Write the grown branch, deepest node first, modifying the arc pointers for the branch processed

      ;
      ; TODO: clean this mess up, recursive build of bytes first then write?
      ;
      (let [file-ptr
            (loop [node (first branch)
                   left (rest branch)
                   file-ptr file-ptr]
              (if node
                (recur
                  (first left)
                  (rest left)
                  (io/write-bytes
                    db
                    (marshal-node
                      (set-arc node (nth hashes (:depth node)) (- file-ptr (node-size)))
                      file-ptr)
                    file-ptr))
                file-ptr))]
        (io/save-root-node-ptr db (- file-ptr (node-size)))))
    db)

;;;;;;;;;;;;;;;;;; hamt fetch ;;;;;;;;;;;;;;;;;;

(defn fetch
  "Fetch a value for a key, returns nil if not found"
  [db key]
    (let [node (first (node-path db (io/root-node-ptr db) (hash-codes key) 0))]
      (cond
        (nil? node) nil
        (= (name key) (:key node)) (:value node)
        :else nil)))
