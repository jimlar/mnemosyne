(ns storage.io-test
  (:use midje.sweet)
  (:use storage.io))

(fact "hexread decodes 4711 properly"
  (apply list (read-bytes (hexread "4711") 2))
  => [(byte 0x47) (byte 0x11)])
  
(fact "hexdump dumps 4711 properly"
  (hexdump (byte-array [(byte 0x47) (byte 0x11)]))
  => "4711")

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

(fact "leaf node are packed to marshalled key/value and zero bitmap"
  (hexdump (marshal-node (leaf "a" "b") 0))
  => "000000016100000001620000000000000000")

(fact "the offset is added to key/value back reference"
  (hexdump (marshal-node (leaf "a" "b") 4711))
  => "000000016100000001620000126700000000")

(fact "leaf node can be unmarshalled"
  (unmarshal-node (hexread "000000016100000001620000000000000000") 10)
  => (leaf "a" "b"))
