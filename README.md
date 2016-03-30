
A set of tools to help in understading cassandra database storage internals. More detailed description of storage internals can be found at http://distributeddatastore.blogspot.com

token.py - This script converts a given key to token using RandomPartitioner

sstable.py - This script provides common classes to parse SSTable component files

sstable-metadata.py - This script reads the SSTable stats file to display SSTable metadata information. It supports version "ha" onwards. 

sstable-index.py - This script reads the SSTable index file to display SSTable row index entries. It is tested with version "jb" 

sstable2json.py - This script reads the rows and columns in a given SSTable and converts those to JSON format similar to sstable2json tool. It doesn't require access to cassandra column families in system keyspace to decode SSTable data like sstable2json tool. It is tested with version "ic" and "jb"
