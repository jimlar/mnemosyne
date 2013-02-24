(ns mnemosyne.server-test
  (:use midje.sweet)
  (:use mnemosyne.server))

(fact "parse line gives empty vector on bad line"
  (parse-line "") => []
  (parse-line " ") => []
  (parse-line " put") => []
  (parse-line "\n") => [])

(fact "parse line supports no arg command"
  (parse-line "reload ") => ["RELOAD"]
  (parse-line "reload") => ["RELOAD"])

(fact "parse line supports command with arguments"
  (parse-line "put a") => ["PUT" "a"]
  (parse-line "put a b") => ["PUT" "a b"])
