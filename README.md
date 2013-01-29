# storage

The database that never forgets

Note: this is work in progress, expect everything to blow up.

## Status
* Lock free concurrent read/write on disk HAMT almost there

## TODO

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
