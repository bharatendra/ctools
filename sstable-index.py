import sys
import os
from buffer import Buffer


class IndexInfo:
    def __init__(self, entries):
        self.entries = entries
        self.rowcount = len(self.entries)

    def parse(self, filename):
        size = os.stat(filename).st_size
        remaining = size
        f = open(filename, 'r')
        buf = Buffer(f.read())
        f.close()
        entries = []
        while buf.remaining() > 0:
            key = buf.unpack_utf_string()
            pos = buf.unpack_longlong()
            buf.skip_data()
            entries.append((key, pos))
        return IndexInfo(entries)
    parse = classmethod(parse)

    def __repr__(self):
        out = ""
        for i in xrange(len(self.entries)):
            s = "key: %s pos: %s\n" % (self.entries[i][0], self.entries[i][1])
            out += s
        s = "row count: %d" % len(self.entries)
        out += s
        return out
            

if len(sys.argv) < 2:
    print "Usage: python sstable-index.py <index file>"
    sys.exit(1)

index = IndexInfo.parse(sys.argv[1])
print index
