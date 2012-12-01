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

(defn hexdump [barray]
  "Create a hex-string from a byte array"
  (apply str (map #(with-out-str (printf "%02x" %)) barray)))

(defn hexread [s]
  "Convert hex-string to a seekable input stream"
  (java.nio.ByteBuffer/wrap 
    (byte-array 
      (map #(byte (Integer/parseInt % 16)) 
           (map #(apply str %) 
                (partition-all 2 s))))))

(defn node [children]
  "A HAMT node with children"
  {:arcs (vector children)})

(defn leaf [key, value]
  "A HAMT leaf node, with a key and a value"
  {:arcs [] :key key :value value})

(defn leaf? [node]
  "Return true if node is a leaf"
  (empty? (:arcs node)))

(defn marshal-int [i]
  "Turns a 32-bit int into a 4 byte array (assumes big endian)"
  (.array (.putInt (java.nio.ByteBuffer/allocate 4) i)))

(defn unmarshal-int [barray]
  "Turns a 32-bit int into a 4 byte array (assumes big endian)"
  (.getInt (java.nio.ByteBuffer/wrap barray) 0))

(defn marshal-string [s]
  "Turn string into byte sequence for disk storage"
  (let [data (.getBytes s "utf-8")]
    (concat (marshal-int (count data)) data)))

(defn unmarshal-string [in]
  "Read a string"
  (let [len (unmarshal-int (read-bytes in 4))]
    (String. (read-bytes in len) "utf-8")))

(defn marshal-node [node offset]
  "Create bytes from node, offset is added to all pointers"
  (if (leaf? node)
    (concat (marshal-string (:key node)) 
            (marshal-string (:value node))
            (marshal-int offset)
            (marshal-int 0))
    ()))

(defn unmarshal-node [in position]
  "Read node from bytes"
  (seek in position)
  (let [pointer (unmarshal-int (read-bytes in 4))
        arcbits (unmarshal-int (read-bytes in 4))]
    (seek in pointer)
    {:key (unmarshal-string in) :value (unmarshal-string in) :arcs []}))

