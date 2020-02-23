
A set of tools to help in understading cassandra database storage internals. More detailed description of storage internals can be found at http://distributeddatastore.blogspot.com

gettoken.py - This script converts a given key to token using RandomPartitioner

token-hexkey.py - This script converts a given key in hex format to token using RandomPartitioner

sstable.py - This script provides common classes to parse SSTable component files

sstable-metadata.py - This script reads the SSTable stats file to display SSTable metadata information. It supports version "ha" onwards. Version 3.11 also supported.

sstable-index.py - This script reads the SSTable index file to display SSTable row index entries. It is tested with version "jb" 

sstable2json.py - This script reads the rows and columns in a given SSTable and converts those to JSON format similar to sstable2json tool. It doesn't require access to cassandra column families in system keyspace to decode SSTable data like sstable2json tool. It is tested with version "ic","jb" and "ka"

Example: 

$ python sstable2json.py -c data/lb/irisplot-dbf35720528411eabbb8b16d9d604ffd/lb-1-big-Data.db
[
{"key": "40966666",
 "cells": [["40e00000:00000003:636f6c6f72","677265656e",1582054414657067]]},
{"key": "3fb33333",
 "cells": [["409ccccd:00000002:636f6c6f72","726564",1582054358578646],
	["40a33333:00000001:","",1582054337118746],
	["40a33333:00000001:636f6c6f72","726564",1582054337118746]]},
{"key": "40a33333",
 "cells": [["40b9999a:00000006:636f6c6f72","626c7565",1582054298177996]]},
{"key": "40c00000",
 "cells": [["40c9999a:00000005:636f6c6f72","626c7565",1582054268167535]]},
{"key": "40900000",
 "cells": [["40cccccd:00000004:636f6c6f72","677265656e",1582054399453891]]}
]

$ ppython sstable2json.py -c data/lb/iris-9cb598404fd011eabbb8b16d9d604ffd/lb-1-big-Data.db 
[
{"key": "00000005",
 "cells": [["636c617373","497269732d76697267696e696361",1581757206044154],
	["706574616c6c656e677468","40c00000",1581757206044154],
	["706574616c7769647468","40200000",1581757206044154],
	["736570616c6c656e677468","40c9999a",1581757206044154],
	["736570616c7769647468","40533333",1581757206044154]]},
{"key": "00000001",
 "cells": [["636c617373","497269732d7365746f7361",1581756951102675],
	["706574616c6c656e677468","3fb33333",1581756951102675],
	["706574616c7769647468","3e4ccccd",1581756951102675],
	["736570616c6c656e677468","40a33333",1581756951102675],
	["736570616c7769647468","40600000",1581756951102675]]},
{"key": "00000002",
 "cells": [["636c617373","497269732d7365746f7361",1581757185025435],
	["706574616c6c656e677468","3fb33333",1581757185025435],
	["706574616c7769647468","3e4ccccd",1581757185025435],
	["736570616c6c656e677468","409ccccd",1581757185025435],
	["736570616c7769647468","40400000",1581757185025435]]},
{"key": "00000004",
 "cells": [["636c617373","497269732d7665727369636f6c6f72",1581757198372339],
	["706574616c6c656e677468","40900000",1581757198372339],
	["706574616c7769647468","3fc00000",1581757198372339],
	["736570616c6c656e677468","40cccccd",1581757198372339],
	["736570616c7769647468","404ccccd",1581757198372339]]},
{"key": "00000006",
 "cells": [["636c617373","497269732d76697267696e696361",1581757212107918],
	["706574616c6c656e677468","40a33333",1581757212107918],
	["706574616c7769647468","3ff33333",1581757212107918],
	["736570616c6c656e677468","40b9999a",1581757212107918],
	["736570616c7769647468","402ccccd",1581757212107918]]},
{"key": "00000003",
 "cells": [["636c617373","497269732d7665727369636f6c6f72",1581757191320430],
	["706574616c6c656e677468","40966666",1581757191320430],
	["706574616c7769647468","3fb33333",1581757191320430],
	["736570616c6c656e677468","40e00000",1581757191320430],
	["736570616c7769647468","404ccccd",1581757191320430]]}
]

