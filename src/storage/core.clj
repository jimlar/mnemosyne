(ns storage.core)

(defn connect []
  (atom {}))

(defn store [db key value]
  (swap! db assoc key value))

(defn fetch [db key]
  (get @db key))
