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
    (let [node (io/unmarshal-node (:data @db) (io/root-node (:data @db)))]
      (if (= (name key) (:key node))
        (:value node)
        nil))))

