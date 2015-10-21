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

# a stand alone script to read rows and columns in a given SSTable

import sys
import os
from buffer import Buffer

class SSTableMetadata:
    def __init__(self, rowsizes, colcounts, replaysegid, replaypos, tsmin, tsmax, maxlocaldeletiontime, bloomfilterfpchance, compressionratio, partitioner, ancestors, tombstonehistogram, sstablelevel, mincolnames, maxcolnames):
        self.rowsizes = rowsizes
        self.colcounts = colcounts
        self.replaysegid = replaysegid
        self.replaypos = replaypos
        self.tsmin = tsmin
        self.tsmax = tsmax
        self.maxlocaldeletiontime = maxlocaldeletiontime
        self.bloomfilterfpchance = bloomfilterfpchance
        self.compressionratio = compressionratio
        self.partitioner = partitioner
        self.ancestors = ancestors
        self.tombstonehistogram = tombstonehistogram
        self.sstablelevel = sstablelevel
        self.mincolnames = mincolnames
        self.maxcolnames = maxcolnames

    def parse(self, filename):
        size = os.stat(filename).st_size
        remaining = size
        f = open(filename, 'r')
        buf = Buffer(f.read())
        f.close()
        rowsizes = SSTableMetadata.unpack_estimated_histogram(buf)
        colcounts = self.unpack_estimated_histogram(buf)
        replaysegid = buf.unpack_longlong()
        replaypos = buf.unpack_int()
        tsmin = buf.unpack_longlong()
        tsmax = buf.unpack_longlong()
        maxlocaldeletiontime = buf.unpack_int()
        bloomfilterfpchance = buf.unpack_double()
        compressionratio = buf.unpack_double()
        partitioner = buf.unpack_utf_string()
        ancestorscount = buf.unpack_int()
        ancestors = []
        for i in xrange(ancestorscount):
            ancestors.append(buf.unpack_int())
        tombstonehistogram = self.unpack_streaming_histogram(buf)
        sstablelevel = 0;
        if (buf.available()):
            sstablelevel = buf.unpack_int()
        mincolnames = []
        maxcolnames = []
        count = buf.unpack_int()
        for i in xrange(count):
            mincolnames.append(buf.unpack_utf_string())
        count = buf.unpack_int()
        for i in xrange(count):
            maxcolnames.append(buf.unpack_utf_string())
        return SSTableMetadata(rowsizes, colcounts, replaysegid, replaypos, tsmin, tsmax, maxlocaldeletiontime, bloomfilterfpchance, compressionratio, partitioner, ancestors, tombstonehistogram, sstablelevel, mincolnames, maxcolnames)
    parse = classmethod(parse)

    def unpack_estimated_histogram(self, buf):
        size = buf.unpack_int()
        offsets = [0 for i in xrange(size - 1)]
        buckets = [0 for i in xrange(size)]
        for i in xrange(size):
            if i == 0:
                offsets[0] = buf.unpack_longlong()
            else:
                offsets[i - 1] = buf.unpack_longlong()
            buckets[i] = buf.unpack_longlong()
        return (offsets, buckets)
    unpack_estimated_histogram = classmethod(unpack_estimated_histogram)

    def unpack_streaming_histogram(self, buf):
        maxbinsize = buf.unpack_int()
        size = buf.unpack_int()
        bins = {}
        for i in xrange(size):
            point = buf.unpack_double()
            count = buf.unpack_longlong()
            bins[point] = count
        return (maxbinsize, bins)
    unpack_streaming_histogram = classmethod(unpack_streaming_histogram)

    def __repr__(self):
        return "rowSizes: %s\ncolumnCounts: %s\nreplaySegId: %d\nreplayPosition: %d\nminTimestamp: %d\nmaxTimestamp: %d\nmaxLocalDeletionTime: %d\nbloomFilterFPChance: %f\ncompressionRatio: %f\npartitioner: %s\nancestors: %s\ntombstoneHistogram: %s\nsstableLevel: %d\nminColumnNames: %s\nmaxColumnNames: %s\n" % (self.rowsizes, self.colcounts, self.replaysegid, self.replaypos, self.tsmin, self.tsmax, self.maxlocaldeletiontime, self.bloomfilterfpchance, self.compressionratio, self.partitioner, self.ancestors, self.tombstonehistogram, self.sstablelevel, self.mincolnames, self.maxcolnames)

if len(sys.argv) < 2:
    print "Usage: python sstable-metadata.py <stats file>"
    sys.exit(1)

metadata = SSTableMetadata.parse(sys.argv[1])
print metadata
