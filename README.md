# MnemosyneDB

The database that never forgets

Note: this is work in progress, expect everything to blow up.

http://en.wikipedia.org/wiki/Mnemosyne

## Done
* Lock free concurrent read/write on disk HAMT structure

## TODO / Open questions

* Memory map in blocks, one block for existing file and extend with new chunks
* Agent to serialize the writes
* Duplicated mmapped buffer pool for reads
* Should I do distributon and will the on disk HAMT work with replication?
    * Synchronization, vector clocks, distributes time - NTP?
* Transactions or not?
* Key/hint-tables in memory?
* Gloss for the binary encoding/decoding?
* Add go back in history API
* Introduce "reference" type for pointer to create/serialize branches completely before writing them? (less logic in the store function)

## Assumptions/decisions

* Big endian on disk
* All strings UTF-8

## Usage

FIXME

## License

Copyright © 2012 Jimmy Larsson

Distributed under the Eclipse Public License, the same as Clojure.
