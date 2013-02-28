(ns mnemosyne.server
  (:require [lamina.core :as lamina]
            [aleph.tcp :as aleph]
            [gloss.core :as gloss]
            [clojure.string :as string]))

;;;;;;;;;;;;;;;;;; Protocol commands ;;;;;;;;;;;;;;;;;;

(defmulti execute-command :command)

(defmethod execute-command :default
  [{args :args cmd :command}]
  (if cmd
      (str "Unknown command " (name cmd))
      "You need to supply a command"))

(defmethod execute-command :SET
  [{args :args}]
    (str "You want to SET " args))

;;;;;;;;;;;;;;;;;; Protocol parsing ;;;;;;;;;;;;;;;;;;

(defn parse-line [line]
  (let [split-line (re-find #"^(\w+) ?(.*)$" line)
        command (nth split-line 1)
        command (if (nil? command) nil (string/upper-case command))
        argument (nth split-line 2)]
    (cond
      (= 0 (count command)) {}
      (= 0 (count argument)) {:command (keyword command)}
      :else {:command (keyword command) :args argument})))

(defn parse-and-execute [line]
  (execute-command (parse-line line)))

;;;;;;;;;;;;;;;;;; TCP Server ;;;;;;;;;;;;;;;;;;

(defn start-server
  "Start a server on port passing each input line to handler with responds by returning a string response"
  [port line-handler]
  (aleph/start-tcp-server
    (fn [ch client-info] (lamina/receive-all ch #(lamina/enqueue ch (line-handler %))))
    {:port port, :frame (gloss/string :utf-8 :delimiters ["\r\n"])})
  (print (str "Started on port " port "\n")))

(defn -main [& m]
  (start-server 10000 parse-and-execute))
