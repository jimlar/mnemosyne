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
  (parse-line "reload") => {:command :reload})

(fact "parse line supports command with arguments"
  (parse-line "put a") => {:command :put :args "a"}
  (parse-line "put a b") => {:command :put :args "a b"})
