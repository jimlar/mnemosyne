(ns storage.test-utils)

(defn- byte->hexstr [b]
  (let [s (Integer/toString b 16)]
    (if (= 1 (count s))
      (str "0" s)
      s)))

(defn hexdump [barray]
  "Create a hexdump string from the bytes"
  (apply str (map byte->hexstr barray)))
