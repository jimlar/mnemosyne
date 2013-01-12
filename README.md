# storage

The database that never forgets

## TODO

* Disk storage format ideas: 
  * HAMT, Hash Array Mapped Tries - Phil Bagwell
  * Bitcask http://docs.basho.com/riak/latest/tutorials/choosing-a-backend/Bitcask/
  * Redis - RDB https://github.com/sripathikrishnan/redis-rdb-tools/wiki/Redis-RDB-Dump-File-Format
  * LSM-trees
  * Radix trees
  * RRB-trees - för vektorer
  * Log structured storage (CouchDB) http://blog.notdot.net/2009/12/Damn-Cool-Algorithms-Log-structured-storage
  * BerkleyDB JE http://www.oracle.com/technetwork/database/berkeleydb/overview/index-093405.html  
* Key/hint-tables in memory?
* Synchronization, vector clocks, distributes time - NTP
* Transactions or not?
* How to manage files open for read and write

* Use gloss for binary: https://github.com/ztellman/gloss/wiki/Introduction

## Asumptions/decisions

* Big endian on disk
* All strings UTF-8

## Usage

FIXME

## License

Copyright © 2012 Jimmy Larsson

Distributed under the Eclipse Public License, the same as Clojure.
