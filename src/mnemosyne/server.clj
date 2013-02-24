(ns mnemosyne.server
  (:require [lamina.core :as lamina]
            [aleph.tcp :as aleph]
            [gloss.core :as gloss]
            [clojure.string :as string]))


;;;;;;;;;;;;;;;;;; Line protocol ;;;;;;;;;;;;;;;;;;

(defn parse-line [line]
  (let [split-line (re-find #"^(\w+) ?(.*)$" line)
        command (nth split-line 1)
        command (if (nil? command) nil (string/upper-case command))
        argument (nth split-line 2)]
    (cond
      (= 0 (count command)) []
      (= 0 (count argument)) [command]
      :else [command argument])))

(defn execute-command [line]
  (let [command (parse-line line)]
    (str "You said to execute " command)))

;;;;;;;;;;;;;;;;;; TCP Server ;;;;;;;;;;;;;;;;;;

(defn start-server
  "Start a server on port passing each input line to handler with responds by returning a string response"
  [port line-handler]
  (aleph/start-tcp-server
    (fn [ch client-info] (lamina/receive-all ch #(lamina/enqueue ch (line-handler %))))
    {:port port, :frame (gloss/string :utf-8 :delimiters ["\r\n"])})
  (print (str "Started on port " port "\n")))

(defn -main [& m]
  (start-server 10000 execute-command))
