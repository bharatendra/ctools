#!/usr/bin/env python
# -*- coding: utf-8 -*-
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

# a stand alone script to read metadata of a given SSTable

import sys
import os
from buffer import Buffer
from sstable import SSTableFileName

class SSTableMetadata:
    version = ''
    rowsizes = []
    colcounts = []
    replaysegid = 0
    replaypos = 0
    tsmin = 0
    tsmax = 0
    maxlocaldeletiontime = 0
    bloomfilterfpchance = 0.0
    compressionratio = 0.0
    partitioner = ''
    ancestors = []
    tombstonehistogram = []
    sstablelevel = 0
    mincolnames = []
    maxcolnames = []
    repiredat = 0
    haslegacycountershards = []
    cardinality = []

    def parse(self, filename, version):
        size = os.stat(filename).st_size
        remaining = size
        f = open(filename, 'r')
        buf = Buffer(f.read())
        f.close()
        metadata = SSTableMetadata()
        metadata.version = version
        if version >= 'ka':
            metadata.parse_metadata_version_ka(buf, version)
        elif version >= 'ja':
            metadata.parse_metadata_version_ja(buf, version)
        elif version >= 'ia':
            metadata.parse_metadata_version_ia(buf, version)
        elif version >= 'ha':
            metadata.parse_metadata_version_ia(buf, version)
        else:
            print "version %s not supported" % (version)
        return metadata
    parse = classmethod(parse)


    def parse_metadata_version_ia(self, buf, version):
        self.rowsizes = SSTableMetadata.unpack_estimated_histogram(buf)
        self.colcounts = SSTableMetadata.unpack_estimated_histogram(buf)
        self.replaysegid = buf.unpack_longlong()
        self.replaypos = buf.unpack_int()
        if version >= 'ib':
            self.tsmin = buf.unpack_longlong()
        if version >= 'hd':
            self.tsmax = buf.unpack_longlong()
        if version >= 'hb':
            self.compressionratio = buf.unpack_double()
        if version >= 'hc':
            self.partitioner = buf.unpack_utf_string()
        if version >= 'he':
            ancestorscount = buf.unpack_int()
            self.ancestors = []
            for i in xrange(ancestorscount):
                self.ancestors.append(buf.unpack_int())
        if version >= 'ia':
            self.tombstonehistogram = self.unpack_streaming_histogram(buf)

    def parse_metadata_version_ja(self, buf, version):
        self.rowsizes = SSTableMetadata.unpack_estimated_histogram(buf)
        self.colcounts = SSTableMetadata.unpack_estimated_histogram(buf)
        self.replaysegid = buf.unpack_longlong()
        self.replaypos = buf.unpack_int()
        self.tsmin = buf.unpack_longlong()
        self.tsmax = buf.unpack_longlong()
        self.maxlocaldeletiontime = buf.unpack_int()
        self.bloomfilterfpchance = buf.unpack_double()
        self.compressionratio = buf.unpack_double()
        self.partitioner = buf.unpack_utf_string()
        ancestorscount = buf.unpack_int()
        self.ancestors = []
        for i in xrange(ancestorscount):
            self.ancestors.append(buf.unpack_int())
        self.tombstonehistogram = self.unpack_streaming_histogram(buf)
        self.sstablelevel = buf.unpack_int()
        self.mincolnames = []
        self.maxcolnames = []
        count = buf.unpack_int()
        for i in xrange(count):
            self.mincolnames.append(buf.unpack_utf_string())
        count = buf.unpack_int()
        for i in xrange(count):
            self.maxcolnames.append(buf.unpack_utf_string())

    def parse_metadata_version_ka(self, buf, version):
        numcomponents = buf.unpack_int()
        toc = {}
        
        for i in xrange(numcomponents):
            type = buf.unpack_int()
            val = buf.unpack_int()
            toc[type] = val
        for j in xrange(3):
            if j in toc:
                buf.seek(toc[j])
                if j == 0:
                    self.partitioner = buf.unpack_utf_string()
                    self.bloomfilterfpchance = buf.unpack_double()
                elif j == 1:
                    ancestorscount = buf.unpack_int()
                    self.ancestors = []
                    for a in xrange(ancestorscount):
                        self.ancestors.append(buf.unpack_int())
                    self.cardinality = buf.unpack_data()
                else:
                    self.rowsizes = SSTableMetadata.unpack_estimated_histogram(buf)
                    self.colcounts = SSTableMetadata.unpack_estimated_histogram(buf)
                    self.replaysegid = buf.unpack_longlong()
                    self.replaypos = buf.unpack_int()
                    self.tsmin = buf.unpack_longlong()
                    self.tsmax = buf.unpack_longlong()
                    self.maxlocaldeletiontime = buf.unpack_int()
                    self.compressionratio = buf.unpack_double()
                    self.tombstonehistogram = self.unpack_streaming_histogram(buf)
                    self.sstablelevel = buf.unpack_int()
                    self.repairedat = buf.unpack_longlong()
                    self.mincolnames = []
                    self.maxcolnames = []
                    count = buf.unpack_int()
                    for i in xrange(count):
                        self.mincolnames.append(buf.unpack_utf_string())
                    count = buf.unpack_int()
                    for i in xrange(count):
                        self.maxcolnames.append(buf.unpack_utf_string())
                    self.haslegacycountershards = buf.unpack_byte()
        

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
        if self.version >= 'ka':
            return "rowSizes: %s\ncolumnCounts: %s\nreplaySegId: %d\nreplayPosition: %d\nminTimestamp: %d\nmaxTimestamp: %d\nmaxLocalDeletionTime: %d\nbloomFilterFPChance: %f\ncompressionRatio: %f\npartitioner: %s\nancestors: %s\ntombstoneHistogram: %s\nsstableLevel: %d\nrepairdAt: %d\nminColumnNames: %s\nmaxColumnNames: %s\nhasLegacyCounterShards: %s\n" % (self.rowsizes, self.colcounts, self.replaysegid, self.replaypos, self.tsmin, self.tsmax, self.maxlocaldeletiontime, self.bloomfilterfpchance, self.compressionratio, self.partitioner, self.ancestors, self.tombstonehistogram, self.sstablelevel, self.repairedat, self.mincolnames, self.maxcolnames, self.haslegacycountershards)
        elif self.version >= 'ja':
            return "rowSizes: %s\ncolumnCounts: %s\nreplaySegId: %d\nreplayPosition: %d\nminTimestamp: %d\nmaxTimestamp: %d\nmaxLocalDeletionTime: %d\nbloomFilterFPChance: %f\ncompressionRatio: %f\npartitioner: %s\nancestors: %s\ntombstoneHistogram: %s\nsstableLevel: %d\nminColumnNames: %s\nmaxColumnNames: %s\n" % (self.rowsizes, self.colcounts, self.replaysegid, self.replaypos, self.tsmin, self.tsmax, self.maxlocaldeletiontime, self.bloomfilterfpchance, self.compressionratio, self.partitioner, self.ancestors, self.tombstonehistogram, self.sstablelevel, self.mincolnames, self.maxcolnames)
        elif self.version >= 'ia':
            return "rowSizes: %s\ncolumnCounts: %s\nreplaySegId: %d\nreplayPosition: %d\nminTimestamp: %d\nmaxTimestamp: %d\ncompressionRatio: %f\npartitioner: %s\nancestors: %s\ntombstoneHistogram: %s\n" % (self.rowsizes, self.colcounts, self.replaysegid, self.replaypos, self.tsmin, self.tsmax, self.compressionratio, self.partitioner, self.ancestors, self.tombstonehistogram)
        
                 
if len(sys.argv) < 2:
    print "Usage: python sstable-metadata.py <stats file>"
    sys.exit(1)

filename = sys.argv[1]
sstable = SSTableFileName.parse(filename)
print sstable
metadata = SSTableMetadata.parse(sys.argv[1], sstable.version)
print metadata
