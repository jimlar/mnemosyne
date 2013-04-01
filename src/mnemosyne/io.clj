(ns mnemosyne.io
  (:require [clojure.java.io :as java-io]))

(declare root-node-ptr)
(declare hexdump)

;;;;;;;;;;;;;;;;;; disk io ;;;;;;;;;;;;;;;;;;

(defn ensure-file
  "Ensure that fhe file i a java.io.File, accepts String and File argument"
  [file]
  (cond
    (instance? java.io.File file) file
    (string? file) (java.io.File. file)
    :else (throw (IllegalArgumentException. (str "expected String or java.io.File, got: " (class file))))))

(defn map-file
  "Create a memory mapped buffer for the file"
  [file pos size]
  (let [raf (java.io.RandomAccessFile. file "rw")
        mapped (.map (.getChannel raf) java.nio.channels.FileChannel$MapMode/READ_WRITE pos size)]
      (.close raf)
      mapped))

(defn open-db
  "Open the on disk database"
  ([] (open-db (str (java.io.File/createTempFile "mnemosyne" ".tmp"))))
  ([file]
    (let [file (ensure-file file)
          root-ptr (map-file file 0 8)
          out (map-file file 0 Integer/MAX_VALUE)]
      {
        :file file
        :root-ptr root-ptr
        :out out
      })))

(defn close-db [db]
  (.close (:out db))
  (.close (:root-ptr db)))

(defn end-pointer [db]
  (let [root (root-node-ptr db)]
    (if (= 0 root)
      8
      (+ 16 root))))

(defn read-bytes [db n]
  (let [bs (byte-array n)]
    (.get (:out db) bs)
    bs))

(defn seek [db pos]
  (.position (:out db) (int pos)))

(defn write-bytes
  "Writes bytes and returns the resulting file pos"
  ([db data] (write-bytes db data (end-pointer db)))
  ([db data pos]
    (seek db pos)
    (.put (:out db) data)
    (+ pos (count data))))

;;;;;;;;;;;;;;;;;; hex dump and read ;;;;;;;;;;;;;;;;;;

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


(defn dump-db [db]
  (let [size (end-pointer db)]
    (seek db 0)
    (hexdump (read-bytes db size))))

(defn fake-db
  "Open a fake db by hexreading the supplied strings"
  [& strs]
  (let [bytes (apply hexreader strs)
        size (.capacity bytes)]
    {:root-ptr bytes :out bytes}))

;;;;;;;;;;;;;;;;;; mashal and unmarshaling ;;;;;;;;;;;;;;;;;;

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

;;;;;;;;;;;;;;;;;; latest root node pointer ;;;;;;;;;;;;;;;;;;

(defn root-node-ptr [db]
  (seek db 0)
  (unmarshal-long db))

(defn save-root-node-ptr [db ptr]
  (write-bytes db (marshal-long ptr) 0))

