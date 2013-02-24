(ns storage.io-test
  (:use midje.sweet)
  (:use storage.io))

(fact "hexreader decodes 4711 properly"
  (apply list (read-bytes (hexreader "4711") 2))
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

(fact "leaf node are packed to marshalled key/value and zero bitmap"
  (hexdump (marshal-node (leaf "a" "b") 0))
  => "0000000161000000016200000000000000000000000000000000")

(fact "the offset is added to key/value back reference"
  (hexdump (marshal-node (leaf "a" "b") 4711))
  => "0000000161000000016200000000000012670000000000000000")

(fact "leaf node can be unmarshalled"
  (unmarshal-node (hexreader "0000000161" "0000000162" "0000000000000000" "0000000000000000") 10)
  => (leaf "a" "b" :pos 10))

(fact "arc-node is marshalled with arc pointer table"
  (hexdump (marshal-node (set-arc (node) 2 4711) 0))
  => "000000000000126700000000000000000000000000000004")

(fact "arc-node is marshalled with arc pointer table and file offset"
  (hexdump (marshal-node (set-arc (node) 2 4711) 3))
  => "000000000000126700000000000000030000000000000004")

(fact "arc-node is marshalled with arc pointer table"
  (unmarshal-node (hexreader "0000000000001267" "0000000000000000" "0000000000000004") 8)
  => (set-arc (node :pos 8) 2 4711))

(fact "root-node read from start of file"
  (root-node-ptr (hexreader "0000000000000010" "0000000161" "0000000162" "0000000000000008" "0000000000000000"))
  => 16)



