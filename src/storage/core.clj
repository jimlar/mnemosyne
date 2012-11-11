(ns storage.core)

(def ^:private mock-db (atom {}))
(defn start [] mock-db)
  
(def ^:private ^:dynamic *db* nil)

(defmacro with-db 
  "Evaludate body with selected database as the current database"
  [db & body] 
  `(binding [*db* ~db] ~@body))

(defn store [key value]
  (swap! *db* assoc key value))

(defn fetch [key]
  (get @*db* key))
