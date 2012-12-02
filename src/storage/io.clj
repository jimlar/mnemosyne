(ns storage.io
  (:require [clojure.java.io :as java-io]))

(defn- ensure-dir [dir]
  (.mkdirs (java-io/file dir)))

(defn- open-output [dir]
  (java-io/output-stream (java-io/file dir "storage.log") :append true))

(defn- open-input [dir]
  (java.io.RandomAccessFile. (java-io/file dir "storage.log") "r"))

(defn open-dir [dir]
  (ensure-dir dir)
  {:log (open-output dir) :data (open-input dir)})

(defn close [db]
  (.close (:log db))
  (.close (:data db)))

(defn write-bytes [db data] 
  (doto (:log db)
    (.write data)
    (.flush)))

(defprotocol SeekableInput
  (seek-to [r pos])
  (read-bs [r bs]))

(extend-type java.io.RandomAccessFile
  SeekableInput
  (seek-to [r pos] (.seek r pos))
  (read-bs [r bs] (.read r bs)))

(extend-type java.nio.ByteBuffer
  SeekableInput
  (seek-to [r pos] (.position r pos))
  (read-bs [r bs] (.get r bs)))

(defn read-bytes [in n]
  (let [bs (byte-array n)]
    (read-bs in bs)
    bs))

(defn seek [in pos]
  (seek-to in pos))

(defn hexdump 
  "Create a hex-string from a byte array"
  [barray]
  (apply str (map #(with-out-str (printf "%02x" %)) barray)))

(defn hexread
  "Convert hex-string to a seekable input stream"
  [s] 
  (java.nio.ByteBuffer/wrap 
    (byte-array 
      (map #(byte (Integer/parseInt % 16)) 
           (map #(apply str %) 
                (partition-all 2 s))))))

(defn node
  "A HAMT node with children"
  [children] 
  {:arcs (vector children)})

(defn leaf
  "A HAMT leaf node, with a key and a value"
  [key, value] 
  {:arcs [] :key key :value value})

(defn leaf?
  "Return true if node is a leaf"
  [node] 
  (empty? (:arcs node)))

(defn marshal-int
  "Turns a 32-bit int into a 4 byte array (assumes big endian)"
  [i]
  (.array (.putInt (java.nio.ByteBuffer/allocate 4) i)))

(defn marshal-long
  "Turns a 64-bit long into a 8 byte array (assumes big endian)"
  [l]
  (.array (.putLong (java.nio.ByteBuffer/allocate 8) l)))

(defn unmarshal-int
  "Turns a 4 byte array into a 32-bit integer (assumes big endian)"
  [barray]
  (.getInt (java.nio.ByteBuffer/wrap barray) 0))

(defn unmarshal-long
  "Turns a 8 byte array into a 64-bit long (assumes big endian)"
  [barray]
  (.getLong (java.nio.ByteBuffer/wrap barray) 0))

(defn marshal-string
  "Turn string into byte sequence for disk storage"
  [s] 
  (let [data (.getBytes s "utf-8")]
    (concat (marshal-int (count data)) data)))

(defn unmarshal-string
  "Read a string"
  [in] 
  (let [len (unmarshal-int (read-bytes in 4))]
    (String. (read-bytes in len) "utf-8")))

(defn marshal-node
  "Create bytes from node, offset is added to all pointers"
  [node offset] 
  (if (leaf? node)
    (concat (marshal-string (:key node)) 
            (marshal-string (:value node))
            (marshal-long offset)
            (marshal-long 0))
    ()))

(defn unmarshal-node
  "Read node from bytes"
  [in position] 
  (seek in position)
    (let [pointer (unmarshal-long (read-bytes in 8))
          arcbits (unmarshal-long (read-bytes in 8))]
      (seek in pointer)
      {:key (unmarshal-string in) :value (unmarshal-string in) :arcs []}))

