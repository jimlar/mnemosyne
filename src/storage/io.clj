(ns storage.io
  (:require [clojure.java.io :as java-io]))

(defn- open-output [file]
  (let [out (java.io.RandomAccessFile. file "rws")]
    (if (= 0 (.length out))
      (doall (repeatedly 8 #(.write out 0))))
    out))

(defn temp-file []
  (java.io.File/createTempFile "storage" ".tmp")) 

(defn open-file [file]
  (open-output file))

(defn close [db]
  (.close db))

(defn end-pointer [db]
  (.length db))

(defn write-bytes 
  ([db data] (write-bytes db data (end-pointer db)))
  ([db data pos] 
    (.seek db pos)
    (.write db data)
    db))

(defprotocol SeekableInput
  (seek-to [r pos])
  (read-bs [r bs]))

(extend-type java.io.RandomAccessFile
  SeekableInput
  (seek-to [r pos] (.seek r pos))
  (read-bs [r bs] (.read r bs)))

(extend-type java.nio.ByteBuffer
  SeekableInput
  (seek-to [r pos] (.position r (int pos)))
  (read-bs [r bs] (.get r bs)))

(defn read-bytes [db n]
  (let [bs (byte-array n)]
    (read-bs db bs)
    bs))

(defn seek [db pos]
  (seek-to db pos))

(defn hexdump 
  "Create a hex-string from a byte array or random access file"
  [barray]
  (if (instance? java.io.RandomAccessFile barray)
    (let [bs (byte-array (.length barray))]
      (seek barray 0)
      (.readFully barray bs)
      (hexdump bs))
    (apply str (map #(with-out-str (printf "%02x" %)) barray))))

(defn hexreader
  "Convert hex-string to a seekable input stream"
  [& strs] 
  (java.nio.ByteBuffer/wrap 
    (byte-array 
      (map #(byte (Integer/parseInt % 16)) 
           (map #(apply str %) 
                (partition-all 2 (apply str strs)))))))

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

(defn marshal-int
  "Turns a 32-bit int into a 4 byte array (assumes big endian)"
  [i]
  (.array (.putInt (java.nio.ByteBuffer/allocate 4) i)))

(defn marshal-long
  "Turns a 64-bit long into a 8 byte array (assumes big endian)"
  [l]
  (.array (.putLong (java.nio.ByteBuffer/allocate 8) l)))

(defn unmarshal-int
  "Reads 4 bytes from in and turns them into a 32-bit integer (assumes big endian)"
  [db]
  (.getInt (java.nio.ByteBuffer/wrap (read-bytes db 4)) 0))

(defn unmarshal-long
  "Reads 8 bytes from in and turns them into a 64-bit long (assumes big endian)"
  [db]
  (.getLong (java.nio.ByteBuffer/wrap (read-bytes db 8)) 0))

(defn marshal-string
  "Turn string into byte sequence for disk storage"
  [s] 
  (let [data (.getBytes s "utf-8")]
    (concat (marshal-int (count data)) data)))

(defn unmarshal-string
  "Read a string"
  [db] 
  (let [len (unmarshal-int db)]
    (String. (read-bytes db len) "utf-8")))

(defn marshal-node
  "Create bytes from node, offset is added to all pointers"
  [node offset] 
  (if (leaf? node)
    (byte-array (concat (marshal-string (name (:key node)))
                        (marshal-string (:value node))
                        (marshal-long offset)
                        (marshal-long 0)))
    (byte-array (concat (apply concat (map marshal-long (filter #(not (nil? %)) (:arcs node))))
                        (marshal-long offset)
                        (marshal-long (:arcbits node))))))

(defn unmarshal-arc-table 
  "Read arc pointer table into arc vector"
  [arcbits db]
  (map 
    (fn [bit] 
      (if (bit-test arcbits bit)
          (unmarshal-long db)
          nil))
    (range 64)))

(defn unmarshal-node
  "Read node from bytes"
  ([db position depth]
    (assoc (unmarshal-node db position) :depth depth))
  ([db position]
    (seek db position)
      (let [pointer (unmarshal-long db)
            arcbits (unmarshal-long db)]
        (seek db pointer)
        (if (= 0 arcbits) ; leaf node or arc node?
          (leaf (unmarshal-string db) (unmarshal-string db) :pos position)
          (node :pos position :arcbits arcbits :arcs (unmarshal-arc-table arcbits db))))))

(defn root-node [db]
  (seek db 0)
  (unmarshal-long db))

(defn set-root-node [db ptr]
  (write-bytes db (marshal-long ptr) 0))

