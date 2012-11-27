(ns storage.io
  (:require [clojure.java.io :as java-io]))

(defprotocol DataFile
  (get-bytes [this n] "read bytes")
  (seek [this pos] "move pointer to pos")
  (close [this]))

(deftype BufferDataFile [buffer]
  DataFile
  (get-bytes [this n] 
    (let [b (byte-array n)]
      (.get (:buffer this) 0 n)
      b))
  (seek [this n] (.position (:buffer this) n))
  (close [this] this))

(deftype DiskDataFile [file]
  DataFile
  (get-bytes [this n] (.get (:file this) n)) 
  (seek [this n] (.seek (:file this) n))
  (close [this] (.close (:file this))))

(defn buffer-data-file [bytes]
  (BufferDataFile. (java.nio.ByteBuffer/wrap bytes)))

(defn disk-data-file [file]
  (DiskDataFile. (java.io.RandomAccessFile. file "r")))

(defn- ensure-dir [dir]
  (.mkdirs (java-io/file dir)))

(defn- open-log [dir]
  (java-io/output-stream (java-io/file dir "storage.log") :append true))

(defn- open-data [dir]
  (disk-data-file (java-io/file dir "storage.log")))

(defn open-dir [dir]
  (ensure-dir dir)
  {:log (open-log dir) :data (open-data dir)})

(defn close [db]
  (.close (:log db))
  (.close (:data db)))

(defn write-log [db data]
  (doto (:log db)
    (.write data)
    (.flush)))

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

(defn marshal-string [s]
  "Turn string into byte sequence for disk storage"
  (let [data (.getBytes s "utf-8")]
    (concat (marshal-int (count data)) data)))

(defn marshal-node [node offset]
  "Create bytes from node, offset is added to all pointers"
  (if (leaf? node)
    (concat (marshal-string (:key node)) 
            (marshal-string (:value node))
            (marshal-int offset)
            (marshal-int 0))
    ()))

(defn unmarshal-node [in]
  "Create bytes from node, offset is added to all pointers"
  ())

(defn hexdump [barray]
  "Create a hexdump string from the bytes"
  (apply str (map #(with-out-str (printf "%02x" %)) barray)))

(defn hexread [s]
  ())