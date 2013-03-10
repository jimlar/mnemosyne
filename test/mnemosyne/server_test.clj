(ns mnemosyne.server-test
  (:use midje.sweet)
  (:use mnemosyne.server))

(fact "parse line gives empty vector on bad line"
  (parse-line "") => {}
  (parse-line " ") => {}
  (parse-line " put") => {}
  (parse-line "\n") => {})

(fact "parse line supports no arg command"
  (parse-line "reload ") => {:command :reload}
  (parse-line "RELOAD") => {:command :RELOAD})

(fact "parse line supports command with arguments"
  (parse-line "put a") => {:command :put :args "a"}
  (parse-line "put a b") => {:command :put :args "a b"})

(fact "unmatched commands give error string back"
  (execute-command {:command :kalle-kula}) => "Unknown command kalle-kula"
  (execute-command {:command nil}) => "You need to supply a command"
  (execute-command {}) => "You need to supply a command")

(fact "executing set actually calls set on the current db"
  (let [db "fake db"]
    (execute-command {:command :set :db db :args "this-is-the-key this is a value with spaces in"}) => "OK"
    (provided
      (#'mnemosyne.hamt/store db "this-is-the-key" "this is a value with spaces in") => db :times 1)))

(fact "executing get actually calls set on the current db"
  (let [db "fake db"]
    (execute-command {:command :get :db db :args "this-is-the-key"}) => "this is the value with spaces in"
    (provided
      (#'mnemosyne.hamt/fetch db "this-is-the-key") => "this is the value with spaces in" :times 1)))

(fact "executing get on non existing key gives empty string"
  (let [db "fake db"]
    (execute-command {:command :get :db db :args "missing key"}) => ""
    (provided
      (#'mnemosyne.hamt/fetch db "missing key") => nil :times 1)))
