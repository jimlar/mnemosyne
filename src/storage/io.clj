(ns storage.io
  (:require [clojure.java.io :as java-io]))

(defn- ensure-dir [dir]
  (.mkdirs (java-io/file dir)))

(defn- open-log [dir]
  (ensure-dir dir)
  (java-io/output-stream (java-io/file dir "storage.log") :append true))

(defn- open-data [dir]
  (ensure-dir dir)
  (java.io.RandomAccessFile. (java-io/file dir "storage.log") "r"))

(defn open-dir [dir]
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
  (byte-array (map #(byte (bit-and 0xff (bit-shift-right i %))) [24 16 8 0])))

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

(defn hexdump [barray]
  "Create a hexdump string from the bytes"
  (apply str (map #(with-out-str (printf "%02x" %)) barray)))
