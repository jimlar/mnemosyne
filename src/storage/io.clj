(ns storage.io
  (:require [clojure.java.io :as java-io]))

(defn temp-file []
  (java.io.File/createTempFile "storage" ".tmp")) 

(defn open-db
  ([] (open-db (str (temp-file))))
  ([file]
    (let [out (java.io.RandomAccessFile. file "rws")]
      (if (= 0 (.length out))
        (doall (repeatedly 8 #(.write out 0))))
      out)))

(defn close-db [db]
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

(defn root-node-ptr [db]
  (seek db 0)
  (unmarshal-long db))

(defn save-root-node-ptr [db ptr]
  (write-bytes db (marshal-long ptr) 0))

