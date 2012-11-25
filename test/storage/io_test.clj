(ns storage.io-test
  (:use midje.sweet)
  (:use storage.test-utils)
  (:use storage.io))
  
(fact "leaf node are packed to marshalled key/value and zero bitmap"
  (hexdump (marshal-node (leaf "a" "b") 0))
  => "000000016100000001620000000000000000")

(fact "the offset is added to key/value back reference"
  (hexdump (marshal-node (leaf "a" "b") 4711))
  => "000000016100000001620000126700000000")

(fact "empty string is marshalled to a 32-bit zero"
  (hexdump (marshal-string ""))
  => "00000000")

(fact "string 'a' is marshalled to a len 1 and utf-8 byte of a"
  (hexdump (marshal-string "a"))
  => "0000000161")
