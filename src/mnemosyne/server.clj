(ns mnemosyne.server
  (:require [lamina.core :as lamina]
            [aleph.tcp :as aleph]
            [gloss.core :as gloss]))

(defn handler [ch client-info]
  (lamina/receive-all ch
    #(lamina/enqueue ch (str "You said " %))))

(defn -main [& m]
  (aleph/start-tcp-server handler {:port 10000, :frame (gloss/string :utf-8 :delimiters ["\r\n"])})
  (print (str "Started on port " 10000 "\n")))
