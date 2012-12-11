(ns storage.core
  (:require [storage.io :as io]))

(def ^:private ^:dynamic *db* nil)

(defmacro with-open-db 
  "Evaluate body with db as the current database"
  [db & body] 
  `(binding [*db* ~db] ~@body (close-db ~db)))

(defn open-db
  ([] (open-db (str (io/temp-dir))))
  ([dir] (agent (io/open-dir dir))))

(defn close-db [db]
  (send-off db io/close)
  (await db)
  db)

(defn hash-code [key]
  (hash (name key)))

(defn node-path [in node-ptr hash-bits]
  (if (= 0 node-ptr)
    []
    (let [node (io/unmarshal-node in node-ptr)]
      (if (io/leaf? node) [node]
        (let [child-ptr ((:arcs node) (bit-and hash-bits 63))]
          (if (nil? child-ptr) [node]
            (conj (node-path in child-ptr (bit-shift-right hash-bits 6)) node)))))))

(defn store 
  ([key value] (store *db* key value))
  ([db key value] 
    (send-off 
      db 
      (fn [db]
        (io/write-bytes db (io/marshal-node (io/leaf key value) (io/end-pointer db)))
        (io/set-root-node db (- (io/end-pointer db) (io/node-size)))))
    (await db)
    db))

(defn fetch 
  ([key] (fetch *db* key))
  ([db key] 
    (let [in (:data @db)
          node (first (node-path in (io/root-node in) (hash-code key)))]
      (cond 
        (nil? node) nil
        (= (name key) (:key node)) (:value node)
        :else nil))))
