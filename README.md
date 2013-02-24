# MnemosyneDB

The database that never forgets

Note: this is work in progress, expect everything to blow up.

http://en.wikipedia.org/wiki/Mnemosyne

## Done
* Lock free concurrent read/write on disk HAMT

## TODO / Open questions

* Will the on disk HAMT work with replication?
* Skip replication?
* Key/hint-tables in memory?
* Synchronization, vector clocks, distributes time - NTP?
* Transactions or not?
* How to manage files open for read and write - mmap?

## Assumptions/decisions

* Big endian on disk
* All strings UTF-8

## Usage

FIXME

## License

Copyright © 2012 Jimmy Larsson

Distributed under the Eclipse Public License, the same as Clojure.
