Apache Cassandra SSTable Offline Tool
-------------------------------------

A set of tools to work with Cassandra SSTable files offline.


Requirements
------------

python2.7

Getting started
---------------

gettoken.py - This script converts a given key to token using RandomPartitioner

token-hexkey.py - This script converts a given key in hex format to token using RandomPartitioner

sstable.py - This script provides common classes to parse SSTable component files

sstable-metadata.py - This script reads the SSTable stats file to display SSTable metadata information. It supports version "ha" onwards. Version 3.11 also supported.

sstable-index.py - This script reads the SSTable index file to display SSTable row index entries. It is tested with version "jb" 

sstable2json.py - This script reads the rows and columns in a given SSTable and converts those to JSON format similar to sstable2json tool. It doesn't require access to cassandra column families in system keyspace to decode SSTable data like sstable2json tool. It is tested with version "ic","jb", "ka" and "lb". It supports parsing CQL data

Examples
--------
$ ./sst.py -m data/lb/iris-9cb598404fd011eabbb8b16d9d604ffd/lb-1-big-Data.db

rowSizes: ([1, 2, 3, 4, 5, 6, 7, 8, 10, 12, 14...

$ ./sst.py -c -d data/lb/iris-9cb598404fd011eabbb8b16d9d604ffd/lb-1-big-Data.db

[
{"key": "00000005",
 "cells": [["636c617373","497269732d76697267696e696361",1581757206044154],
	["706574616c6c656e677468","40c00000",1581757206044154],
	["706574616c7769647468","40200000",1581757206044154],
	["736570616c6c656e677468","40c9999a",1581757206044154],
	["736570616c7769647468","40533333",1581757206044154]]},
...
]

References
----------

More detailed description of storage internals can be found at http://distributeddatastore.blogspot.com
