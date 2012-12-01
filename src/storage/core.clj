(ns storage.core
  (:require [storage.io :as io]))

(def ^:private ^:dynamic *db* nil)

(defmacro with-open-db 
  "Evaluate body with db as the current database"
  [db & body] 
  `(binding [*db* ~db] ~@body (close-db ~db)))

(defn open-db
  ([] (open-db "/tmp/storage"))
  ([dir] (agent (io/open-dir dir))))

(defn close-db [db]
  (send db io/close)
  (await db))

(defn store [key value]
  (send *db* io/write-bytes (.getBytes (str key "=" value \newline) "utf8")))

(defn fetch [key]
  (get @*db* key))
