(ns mnemosyne.io)

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
  (let [raf (java.io.RandomAccessFile. file "rws")
        mapped (.map (.getChannel raf) java.nio.channels.FileChannel$MapMode/READ_WRITE pos size)]
      (.close raf)
      mapped))

(defn open-db
  "Open the on disk database"
  ([] (open-db (str (java.io.File/createTempFile "mnemosyne" ".tmp"))))
  ([file]
    (let [file (ensure-file file)
          root (map-file file 0 8)
          out (map-file file 8 Integer/MAX_VALUE)]
      {
        :file file
        :root root
        :out out
      })))

(defn close-db [db]
  (.close (:out db))
  (.close (:root db)))

(defn end-pointer [db]
  (let [root (root-node-ptr db)]
    (if (= 0 root)
      8
      (+ 16 root))))

(defn read-bytes [db n]
  (let [bs (byte-array n)]
    (.get (:out db) bs)
    bs))

(defn write-bytes
  "Writes bytes and returns the resulting file pos"
  [db data pos]
    (.position (:out db) (int pos))
    (.put (:out db) data)
    (+ pos (count data)))

;;;;;;;;;;;;;;;;;; hex dump and read ;;;;;;;;;;;;;;;;;;

(defn hexdump
  "Create a hex-string from a byte array"
  [barray]
  (apply str (map #(with-out-str (printf "%02x" %)) barray)))

(defn hexreader
  "Convert hex-strings to a byte buffer"
  [& strs] 
  (java.nio.ByteBuffer/wrap 
    (byte-array 
      (map #(byte (Integer/parseInt % 16)) 
           (map #(apply str %) 
                (partition-all 2 (apply str strs)))))))


(defn dump-db [db]
  (let [size (end-pointer db)
        _ (.position (:out db) 0)
        db-data (read-bytes db size)]
    (.position (:root db) 0)
    (.get (:root db) db-data 0 8)
    (hexdump db-data)))

(defn fake-db
  "Open a fake db by hexreading the supplied strings"
  [& strs]
  (let [bytes (apply hexreader strs)
        size (.capacity bytes)]
    {:root bytes :out bytes}))

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
  [db pos]
  (.getInt (:out db) (int pos)))

(defn unmarshal-long
  "Reads 8 bytes from in and turns them into a 64-bit long (assumes big endian)"
  [db pos]
  (.getLong (:out db) (int pos)))

(defn marshal-string
  "Turn string into byte sequence for disk storage"
  [s] 
  (let [data (.getBytes s "utf-8")]
    (concat (marshal-int (count data)) data)))

(defn unmarshal-string
  "Read a string"
  [db pos]
  (let [len (unmarshal-int db pos)]
    (.position (:out db) (+ pos 4))
    (String. (read-bytes db len) "utf-8")))

;;;;;;;;;;;;;;;;;; latest root node pointer ;;;;;;;;;;;;;;;;;;

(defn root-node-ptr [db]
  (.getLong (:root db) 0))

(defn save-root-node-ptr [db ptr]
  (.putLong (:root db) 0 ptr))

