#!/bin/sh
# -*- mode: Python -*-

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specif

# a stand alone script to read row index entries in a given SSTable index file
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
