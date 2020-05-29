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
import os
from buffer import Buffer
from sstable import SSTableFileName
from sstable import IndexSummary
from sstable import CompressionInfo
from datetime import datetime
import time
from pytz import utc
import argparse

class SSTableMetadata:
    descriptor = ''
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
    esminttl = 0
    esmintimestap = 0
    esminlocaldeletiontime = 0
    keytype = ""
    clusteringtypes = []
    staticcols = []
    regularcols = []
    dt = datetime(year=2015, month=9, day=22)
    microepoch = (time.mktime(utc.localize(dt).utctimetuple()) * 1000 * 1000)
    secepoch = time.mktime(utc.localize(dt).utctimetuple())
    summary = None
    compression = None

    def setsummary(self, summary):
        self.summary = summary

    def setcompression(self, compression):
        self.compression = compression

    def parse(self, filename, version):
        size = os.stat(filename).st_size
        remaining = size
        f = open(filename, 'r')
        buf = Buffer(f.read())
        f.close()
        metadata = SSTableMetadata()
        metadata.descriptor = os.path.abspath(filename)
        metadata.version = version
        if version >= 'mc':
            metadata.parse_metadata_version_mc(buf, version)
        elif version >= 'ka':
            metadata.parse_metadata_version_ka(buf, version)
        elif version >= 'ja':
            metadata.parse_metadata_version_ja(buf, version)
        elif version >= 'ia':
            metadata.parse_metadata_version_ia(buf, version)
        elif version >= 'ha':
            metadata.parse_metadata_version_ia(buf, version)
        else:
            print "version %s not supported" % (version)
        summaryfile = filename.replace("Statistics", "Summary")
        summary = IndexSummary.parse(summaryfile)
        metadata.setsummary(summary)
        compressionfile = filename.replace("Statistics", "CompressionInfo")
        compression = CompressionInfo.parse(compressionfile)
        metadata.setcompression(compression)
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
        
    def parse_metadata_version_mc(self, buf, version):
        numcomponents = buf.unpack_int()
        toc = {}
        for i in xrange(numcomponents):
            type = buf.unpack_int()
            val = buf.unpack_int()
            toc[type] = val
        for j in xrange(4):
            if j in toc:
                buf.seek(toc[j])
                if j == 0: # VALIDATION
                    self.partitioner = buf.unpack_utf_string()
                    self.bloomfilterfpchance = buf.unpack_double()
                elif j == 1: # COMPACTION
                    self.cardinality = buf.unpack_data()
                elif j == 2: # STATS
                    self.rowsizes = SSTableMetadata.unpack_estimated_histogram(buf)
                    self.colcounts = SSTableMetadata.unpack_estimated_histogram(buf)
                    self.replaysegid = buf.unpack_longlong()
                    self.replaypos = buf.unpack_int()
                    self.tsmin = buf.unpack_longlong()
                    self.tsmax = buf.unpack_longlong()
                    self.minlocaldeletiontime = buf.unpack_int()
                    self.maxlocaldeletiontime = buf.unpack_int()
                    self.minttl = buf.unpack_int()
                    self.maxttl = buf.unpack_int()
                    self.compressionratio = buf.unpack_double()
                    self.tombstonehistogram = self.unpack_streaming_histogram(buf)
                    self.sstablelevel = buf.unpack_int()
                    self.repairedat = buf.unpack_longlong()
                    self.minclusteringvalues = []
                    self.maxclusteringvalues = []
                    count = buf.unpack_int()
                    for i in xrange(count):
                        self.minclusteringvalues.append(buf.unpack_utf_string())
                    count = buf.unpack_int()
                    for i in xrange(count):
                        self.maxclusteringvalues.append(buf.unpack_utf_string())
                    self.haslegacycountershards = buf.unpack_byte()
                    self.totalcolsset = buf.unpack_longlong()
                    self.totalrows = buf.unpack_longlong()
                    self.commitloglbreplaysegid = buf.unpack_longlong()
                    self.commitloglbreplaypos = buf.unpack_int()
                    self.commitlogintervals = []
                    count = buf.unpack_int()
                    for i in xrange(count):
                        self.commitlogintervals.append((buf.unpack_longlong(), buf.unpack_int()))
                elif j == 3: # HEADER
                    self.esmintimestap = (buf.unpack_vint() + self.microepoch)
                    self.esminlocaldeletiontime = (buf.unpack_vint() + self.secepoch)
                    self.esminttl = buf.unpack_vint()
                    self.keytype = buf.unpack_vintlendata()
                    clusteringtypecount = buf.unpack_vint()
                    for i in xrange(clusteringtypecount):
                        self.clusteringtypes.append(buf.unpack_vintlendata())
                    staticcolcount = buf.unpack_vint()
                    for i in xrange(staticcolcount):
                        name = buf.unpack_vintlendata()
                        value = buf.unpack_vintlendata()
                        self.staticcols.append((name, value))
                    regularcolcount = buf.unpack_vint()
                    for i in xrange(regularcolcount):
                        name = buf.unpack_vintlendata()
                        value = buf.unpack_vintlendata()
                        self.regularcols.append((name, value))

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

    def mean(self, buckets, offsets):
        elements = 0
        sum = 0.0
        for i in xrange(len(buckets)):
            elements = elements + buckets[i]
            sum = sum + (buckets[i] * offsets[i])
        return sum / elements

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
        if self.version >= 'mc':
            return "SSTable: %s\nPartitioner: %s\nBloom Filter FP chance: %f\nMinimum timestamp: %d\nMaximum timestamp: %d\nSSTable min local deletion time: %d\nSSTable max local deletion time: %d\nCompressor: %s\nCompression ratio: %f\nTTL min: %d\nTTL max: %s\nFirst key: %s\nLast key: %s\nminClustringValues: %s\nmaxClustringValues: %s\nSSTable Level: %d\nRepaird at: %d\ncommitLogIntervals: %s\ntotalColumnsSet: %d\ntotalRows: %d\nreplaySegId: %d\nreplayPosition: %d\ntombstoneHistogram: %s\nES cardinalityLength: %d\nES minTTL: %d\nES minLocalDeletionTime: %d\nES minTimestamp: %d\nkeyType: %s\nClusteringTypes: %s\nStaticColumns: %s\nRegularColumns: %s\n" % (self.descriptor,self.partitioner, self.bloomfilterfpchance, self.tsmin, self.tsmax, self.minlocaldeletiontime, self.maxlocaldeletiontime, self.compression.classname, self.compressionratio, self.minttl, self.maxttl, self.summary.first, self.summary.last, self.minclusteringvalues, self.maxclusteringvalues, self.sstablelevel, self.repairedat, self.commitlogintervals, self.totalcolsset, self.totalrows, self.replaysegid, self.replaypos, self.tombstonehistogram, len(self.cardinality), self.esminttl, self.esminlocaldeletiontime,self.esmintimestap, self.keytype, self.clusteringtypes, self.staticcols, self.regularcols)
        elif self.version >= 'ka':
            return "rowSizes: %s\ncolumnCounts: %s\nreplaySegId: %d\nreplayPosition: %d\nminTimestamp: %d\nmaxTimestamp: %d\nmaxLocalDeletionTime: %d\nbloomFilterFPChance: %f\ncompressionRatio: %f\npartitioner: %s\nancestors: %s\ntombstoneHistogram: %s\nsstableLevel: %d\nrepairdAt: %d\nminColumnNames: %s\nmaxColumnNames: %s\nhasLegacyCounterShards: %s\n" % (self.rowsizes, self.colcounts, self.replaysegid, self.replaypos, self.tsmin, self.tsmax, self.maxlocaldeletiontime, self.bloomfilterfpchance, self.compressionratio, self.partitioner, self.ancestors, self.tombstonehistogram, self.sstablelevel, self.repairedat, self.mincolnames, self.maxcolnames, self.haslegacycountershards)
        elif self.version >= 'ja':
            return "rowSizes: %s\ncolumnCounts: %s\nreplaySegId: %d\nreplayPosition: %d\nminTimestamp: %d\nmaxTimestamp: %d\nmaxLocalDeletionTime: %d\nbloomFilterFPChance: %f\ncompressionRatio: %f\npartitioner: %s\nancestors: %s\ntombstoneHistogram: %s\nsstableLevel: %d\nminColumnNames: %s\nmaxColumnNames: %s\n" % (self.rowsizes, self.colcounts, self.replaysegid, self.replaypos, self.tsmin, self.tsmax, self.maxlocaldeletiontime, self.bloomfilterfpchance, self.compressionratio, self.partitioner, self.ancestors, self.tombstonehistogram, self.sstablelevel, self.mincolnames, self.maxcolnames)
        elif self.version >= 'ia':
            return "rowSizes: %s\ncolumnCounts: %s\nreplaySegId: %d\nreplayPosition: %d\nminTimestamp: %d\nmaxTimestamp: %d\ncompressionRatio: %f\npartitioner: %s\nancestors: %s\ntombstoneHistogram: %s\n" % (self.rowsizes, self.colcounts, self.replaysegid, self.replaypos, self.tsmin, self.tsmax, self.compressionratio, self.partitioner, self.ancestors, self.tombstonehistogram)
