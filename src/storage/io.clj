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

(defn byte-buffer [len]
  (java.nio.ByteBuffer/allocate len))

(defn buf->bytes [buf]
  (.flip buf)
  (let [a (byte-array (.remaining buf))]
    (.get buf a)
    a))

(defn node [children]
  "A HAMT node with children"
  {:arcs (vector children)})

(defn leaf [key, value]
  "A HAMT leaf node, with a key and a value"
  {:arcs [] :key key :value value})

(defn leaf? [node]
  "Return true if node is a leaf"
  (empty? (:arcs node)))

(defn marshal-string [s]
  "Turn string into byte sequence for disk storage"
  (let [data (.getBytes s "utf-8")
        buf (byte-buffer (+ 4 (count data)))]
    (.putInt buf (count data))
    (.put buf data)
    (buf->bytes buf)))

(defn pack [node offset]
  "Create bytes from node, offset is added to all pointers"
  (if (leaf? node)
    [0, 0]
    ()))
