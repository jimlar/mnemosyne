(ns mnemosyne.io-test
  (:use midje.sweet)
  (:use mnemosyne.io))

(fact "hexreader decodes 4711 properly"
  (apply list (read-bytes (fake-db "4711") 2))
  => [(byte 0x47) (byte 0x11)])
  
(fact "hexdump dumps 4711 properly"
  (hexdump (byte-array [(byte 0x47) (byte 0x11)]))
  => "4711")

(fact "long is mashalled as big endian words"
  (hexdump (marshal-long 0x4711))
  => "0000000000004711"
  (hexdump (marshal-long 0x00470047))
  => "0000000000470047"
  (hexdump (marshal-long 0x000000a1))
  => "00000000000000a1")

(fact "negative longs are mashalled correctly"
  (hexdump (marshal-long -1))
  => "ffffffffffffffff"
  (hexdump (marshal-long -2))
  => "fffffffffffffffe")

(fact "int is mashalled as big endian words"
  (hexdump (marshal-int 0x4711))
  => "00004711"
  (hexdump (marshal-int 0x00470047))
  => "00470047")

(fact "negative ints are mashalled correctly"
  (hexdump (marshal-int 0x000000a1))
  => "000000a1"
  (hexdump (marshal-int -1))
  => "ffffffff"
  (hexdump (marshal-int -2))
  => "fffffffe")

(fact "empty string is marshalled to a 32-bit zero"
  (hexdump (marshal-string ""))
  => "00000000")

(fact "string 'a' is marshalled to a len 1 and utf-8 byte of a"
  (hexdump (marshal-string "a"))
  => "0000000161")

(fact "root-node read from start of file"
  (root-node-ptr (fake-db "0000000000000010" "0000000161" "0000000162" "0000000000000008" "0000000000000000"))
  => 16)



