(ns storage.io-test
  (:use midje.sweet)
  (:use storage.test-utils)
  (:use storage.io))
  
(fact "leaf node are packed to zero bitmap"
  (pack (leaf "a" "b") 0)
  => [0xcafebabe, 0])

(fact "empty string is marshalled to a 32-bit zero"
  (hexdump (marshal-string ""))
  => "00000000")

(fact "string 'a' is marshalled to a len 1 and utf-8 byte of a"
  (hexdump (marshal-string "a"))
  => "0000000161")
