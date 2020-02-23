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

# a stand alone script to read rows and columns in a given SSTable

import os
import sys
import struct
import binascii
import lz4.block
import re
import binascii 
from buffer import Buffer

LIVE_MASK            = 0x00
DELETION_MASK        = 0x01
EXPIRATION_MASK      = 0x02
COUNTER_MASK         = 0x04
COUNTER_UPDATE_MASK  = 0x08
RANGE_TOMBSTONE_MASK = 0x10
INT_MAX_VALUE = 0x7fffffff
LONG_MIN_VALUE = 0x8000000000000000
BUFFSIZE = 65536

class CompressedBuffer(Buffer):
    def __init__(self, datafile, compfile, verbose):
        self.compmetadata = CompressionInfo.parse(compfile)
        self.verbose = verbose
        if (self.verbose):
            print self.compmetadata
        self.datasize = os.stat(datafile).st_size
        if (self.verbose):
            print " data size %d" % (self.datasize)
        self.file = open(datafile, 'r')
        self.chunkno = 0
        self.remaininglen = self.datasize
        self.buflen = 0
        self.buf = None
        self.offset = 0
        self.buflen = 0

    def seek(self, pos):
        skipchunkcount = pos / self.compmetadata.chunklen
        skipbytes = pos % self.compmetadata.chunklen
        seekpos = self.compmetadata.chunkoffsets[skipchunkcount]
        if (self.verbose):
            print "row-pos: %d chunklen: %d skipchunkcount: %d seekpos: %d skipbytes: %d" % (pos, self.compmetadata.chunklen, skipchunkcount, seekpos, skipbytes)
        self.file.seek(seekpos)
        self.buf = None
        self.offset = 0
        self.buflen = 0
        self.chunkno = skipchunkcount
        self.rebuffer()
        self.offset = skipbytes

    def rebuffer(self):
        if (self.verbose):
            print "buflen: %d offset: %d" % (self.buflen, self.offset)        
        self.buf = self.nextchunk()
        assert self.buf != None
        self.offset = 0
        self.buflen = len(self.buf)
        if (self.verbose):
            print "buflen: %d" % (self.buflen)

    def endofdata(self):
        if self.remaining() == 0 and self.chunkno >= self.compmetadata.chunkcount:
            return True
        return False

    def nextchunk(self):
        if self.chunkno >= self.compmetadata.chunkcount:
            return
        if (self.chunkno + 1 < self.compmetadata.chunkcount):
            chunklen = self.compmetadata.chunkoffsets[self.chunkno + 1] - self.compmetadata.chunkoffsets[self.chunkno]
            chunk = self.file.read(chunklen)
        else:
            chunk = self.file.read()
        if (self.verbose):
            print "chunklen: ", len(chunk)
        self.chunkno += 1
        newbuf = bytearray('')
        if self.remaining() > 0:
            if (self.verbose):
                print "remaining: %d remaining data: %s" % (self.remaining(), self.get_remaining())
            b1 = self.get_remaining()
            newbuf.extend(b1)
        b = self.uncompress_chunk(chunk)
        if (self.verbose):
            print "uncompressed chunklen: ", len(b)
            self.hexdump(b)
        newbuf.extend(b)
        #newbuf = self.uncompress_chunk(chunk)
        return str(newbuf)

    def uncompress_chunk(self, compressed):
        # skip checksum
        data = compressed[0:len(compressed)-4]
        uncompressed = lz4.block.decompress(data)
        return uncompressed

    def printbinary(self, b):
        s = []
        for i in xrange(len(b)):
            s.append("%s" % b[i])
        s1 = "%s" % s
        s = s1.replace('\', \'\\x', ' ')
        s = s.replace('\', \'', ' ')
        print s

    def hexdump(self, b):
        f=open("./chunk", "wb")
        f.write(b)
        f.close()

class UncompressedBuffer(Buffer):
    def __init__(self, datafile, verbose):
        self.datasize = os.stat(datafile).st_size
        self.file = open(datafile, 'r')
        self.buf = None
        self.offset = 0
        self.buflen = 0
        self.nextchunk = 0
        self.verbose = verbose

    def rebuffer(self):
        if (self.verbose):
            print "buflen: %d offset: %d" % (self.buflen, self.offset)
        newbuf = bytearray('')
        if self.remaining() > 0:
            if (self.verbose):
                print "remaining: %d remaining data: %s" % (self.remaining(), self.get_remaining())
            b1 = self.get_remaining()
            newbuf.extend(b1)
        if self.nextchunk + BUFFSIZE < self.datasize:
            newbuf.extend(self.file.read(BUFFSIZE))
            self.nextchunk = self.nextchunk + BUFFSIZE
        else:
            newbuf.extend(self.file.read())
            self.nextchunk = self.datasize
        self.buf = str(newbuf)
        #assert self.buf != None
        self.buflen = len(self.buf)
        self.offset = 0
        if (self.verbose):
            print "buflen: %d nextchunk: %d datasize: %d" % (self.buflen, self.nextchunk, self.datasize)

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

class IndexSummary:
     def __init__(self, offsetCount, fullSamplingSummarySize, minIndexInterval, samplingLevel, first, last):
         self.offsetCount = offsetCount
         self.fullSamplingSummarySize = fullSamplingSummarySize
         self.minIndexInterval = minIndexInterval
         self.samplingLevel = samplingLevel
         self.first = first
         self.last = last

     def parse(self, filename):
         size = os.stat(filename).st_size
         remaining = size
         f = open(filename, 'r')
         buf = Buffer(f.read())
         f.close()
         minIndexInterval = buf.unpack_int()
         offsetCount = buf.unpack_int()
         offheapSize = buf.unpack_longlong()
         samplingLevel = buf.unpack_int()
         fullSamplingSummarySize = buf.unpack_int()
         buf.skip_bytes(offsetCount * 4)
         buf.skip_bytes(offheapSize - offsetCount * 4);
         first = buf.unpack_data()
         last = buf.unpack_data()
         return IndexSummary(offsetCount, fullSamplingSummarySize, minIndexInterval, samplingLevel, first, last)
     parse = classmethod(parse)

class CompressionInfo:
    def __init__(self, classname, pc, params, datalen, clen, cc, offsets):
        self.classname = classname
        self.paramcount = pc
        self.params = params
        self.uncompressedlen = datalen
        self.chunklen = clen
        self.chunkcount = cc
        self.chunkoffsets = offsets

    def parse(self, filename):
        size = os.stat(filename).st_size
        remaining = size
        f = open(filename, 'r')
        buf = Buffer(f.read())
        f.close()
        classname = buf.unpack_utf_string()
        paramcount = buf.unpack_int()
        params = {}
        for i in xrange(paramcount):
            name = buf.unpack_utf_string()
            value = buf.unpack_utf_string()
            params[name] = value
        chunklen = buf.unpack_int()
        uncompressedlen = buf.unpack_longlong()
        chunkcount = buf.unpack_int()
        offsets = []
        for i in xrange(chunkcount):
            chunkoffset = buf.unpack_longlong()
            offsets.append(chunkoffset)
        return CompressionInfo(classname, paramcount, params, uncompressedlen, chunklen, chunkcount, offsets)
    parse = classmethod(parse)

    def __repr__(self):
        return "class: %s paramcount: %d chunklen: %d uncompressedlen: %d chunkcount: %d" % (self.classname, self.paramcount, self.chunklen, self.uncompressedlen, self.chunkcount)

class SSTableReader:
    def __init__(self, indexfile, datafile, compfile, compressed, cqlrow, verbose):
        self.index = IndexInfo.parse(indexfile)
        if compressed:
            self.buf = CompressedBuffer(datafile, compfile, verbose)
        else:
            self.buf = UncompressedBuffer(datafile, verbose)
        self.entryindex = 0
        self.currow = None
        self.sstable = SSTableFileName.parse(datafile, verbose)
        self.cqlrow = cqlrow
        self.verbose = verbose

    def hasnext(self):
        if self.currow != None:
            while (self.currow.hasnextcolumn()):
                self.currow.nextcolumn()
        return self.entryindex < self.index.rowcount

    def next(self):
        i = self.entryindex
        self.entryindex += 1
        if i + 1 < self.index.rowcount:
            rowsize = self.index.entries[i + 1][1] - self.index.entries[i][1]
        else:
            rowsize = self.buf.datasize
        self.currow = Row(self.index.entries[i], rowsize, self, self.verbose)
        return self.currow

    def seek(self, rowkey):
        for i in xrange(self.index.rowcount):
            if self.index.entries[i][0].find(rowkey) != -1:
                self.entryindex = i + 1
                if i + 1 < self.index.rowcount:
                    rowsize = self.index.entries[i + 1][1] - self.index.entries[i][1]
                else:
                    rowsize = self.buf.datasize
                self.buf.seek(self.index.entries[i][1])
                self.currow = Row(self.index.entries[i], rowsize, self)
                return self.currow

    def unpack_deletion_time(self):
        localDeletionTime = self.buf.unpack_int()
        markedForDeleteAt = self.buf.unpack_longlong()
        return DeletionTime(markedForDeleteAt, localDeletionTime)

    def unpack_column_name(self):
        if (self.cqlrow):
            return self.unpack_composite_column_name()
        name = self.buf.unpack_utf_string()
        if (self.verbose):
            print "\ncolumn name: %s" % (name)        
        return name

    def unpack_composite_column_name(self):
        l = self.buf.unpack_short()
        if (self.verbose):
            print "composite column length: ",l
        if l == 0:
            return None
        name = ""
        pos = self.buf.offset
        firstcomp = True
        while self.buf.offset < pos + l:
            if (self.verbose):
                print "buf offset: ",self.buf.offset
            if firstcomp == False:
                name += ":"
            else:
                firstcomp = False
            cl = self.buf.unpack_short()
            if (self.verbose):
                print "component len: ",cl
            if cl != 0:
                name += binascii.hexlify(self.buf.unpack_bytes(cl))
            #name += self.buf.unpack_utf_string()
            if (self.verbose):
                print "clustering key: ",name
            self.buf.unpack_byte()                
            
        if (self.verbose):
            print "\ncolumn name: %s" % (name)
        return name

    def unpack_column_value(self, name):
        flag = self.buf.unpack_byte()
        if (self.verbose):
            print "column type: 0x%02x" % (flag)
        if (flag & RANGE_TOMBSTONE_MASK) != 0:
            maxcol = self.buf.unpack_utf_string()
            deletiontime = self.unpack_deletion_time()
            return RangeTombstone(name, maxcol, deletiontime)
        else:
            if ((flag & COUNTER_MASK) != 0):
                timestampOfLastDelete = self.buf.unpack_longlong()
                ts = self.buf.unpack_longlong()
                value = self.buf.unpack_data()
                return CounterColumn(name, ts, value, timestampOfLastDelete)
            elif (flag & EXPIRATION_MASK) != 0:
                ttl = self.buf.unpack_int()
                expiration = self.buf.unpack_int()
                ts = self.buf.unpack_longlong()
                value = self.buf.unpack_data()
                return ExpiringColumn(name, ts, ttl, expiration, value)
            else:
                ts = self.buf.unpack_longlong()
                value = self.buf.unpack_data()
                if (flag & COUNTER_UPDATE_MASK) != 0:
                    return CounterUpdateColumn(name, ts, value)
                elif (flag & DELETION_MASK) != 0:
                    return DeletedColumn(name, ts, value)
                else:
                    return Column(name, LIVE_MASK, ts, value)


class Row:
    def __init__(self, indexentry, size, reader, verbose):
        self.indexentry = indexentry
        self.size = size
        self.reader = reader
        self.columncount = 0
        self.verbose = verbose
        
        self.key = self.reader.buf.unpack_utf_string()
        if self.reader.sstable.version < 'd':
            self.size = self.reader.buf.unpack_int()
        elif self.reader.sstable.version < 'ja':
            self.size = self.reader.buf.unpack_longlong()
        self.deletiontime = self.reader.unpack_deletion_time()
        if self.reader.sstable.version < 'ja':
            self.columncount = self.reader.buf.unpack_int()
        self.colname = None
        self.eof = False
        if self.reader.sstable.version < 'ja':
            if self.columncount == 0:
                self.eof = True
        self.colscannedcount = 0
        if reader.cqlrow:
            if (self.verbose):
                print "parsing CQL Row"
            if (self.verbose):
                print "parsing clustering key"
            self.reader.unpack_composite_column_name()
            # skip the column type
            self.reader.buf.skip_bytes(1)
            # read timestamp
            ts = self.reader.buf.unpack_longlong()
            # read empty component
            self.reader.buf.skip_bytes(3)
            # skip the column type
            self.reader.buf.skip_bytes(1)
            

    def hasnextcolumn(self):
        if (self.verbose):
            print "hasnextcolumn"
        if self.columncount > 0 and self.colscannedcount >= self.columncount:
            self.eof = True
        if self.eof == True:
            return False
        self.colname = self.reader.unpack_column_name()
        if self.colname == None or self.colname == "":
            self.eof = True
            self.colname = None
        else:
            self.colscannedcount = self.colscannedcount + 1
        return self.colname != None

    def nextcolumn(self):
        return self.reader.unpack_column_value(self.colname)

    def getdeletioninfo(self):
        return self.deletiontime

class Column:
    def __init__(self, name, type, ts, value):
        self.name = name
        self.type = type
        self.ts = ts
        self.value = value
	if value is None:
		self.value = ''

class CounterColumn(Column):
    def __init__(self, name, ts, value, timestampOfLastDelete):
        Column.__init__(self, name, COUNTER_MASK, ts, value)
        self.timestampOfLastDelete = timestampOfLastDelete

class CounterUpdateColumn(Column):
    def __init__(self, name, ts, value):
        Column.__init__(self, name, COUNTER_UPDATE_MASK, ts, value)
        

class ExpiringColumn(Column):
    def __init__(self, name, ts, ttl, expiration, value):
        Column.__init__(self, name, EXPIRATION_MASK, ts, value)
        self.ttl = ttl
        self.expiration = expiration

class DeletedColumn(Column):
    def __init__(self, name, ts, value):
        Column.__init__(self, name, DELETION_MASK, ts, value)

class RangeTombstone:
    def __init__(self, mincol, maxcol, deletiontime):
        self.mincol = mincol
        self.maxcol = maxcol
        self.deletiontime = deletiontime

class DeletionTime:
    def __init__(self, markedForDeleteAt, localDeletionTime):
        self.markedForDeleteAt = markedForDeleteAt
        self.localDeletionTime = localDeletionTime

    def islive(self):
        return self.markedForDeleteAt == LONG_MIN_VALUE and self.localDeletionTime == INT_MAX_VALUE
 
class SSTableFileName:
    def __init__(self, ks, cf, version, generation, component):
        self.keyspace = ks
        self.columnfamily = cf
        self.version = version
        self.generation = generation
        self.component = component

    def parse(self, filename, verbose):
        if (verbose):
            print filename
        name = os.path.basename(filename)
        m = re.compile(r'(.*)-(.*)-(.*)-(.*)-(.*).db').match(name)
        if m != None:
            ks = m.groups()[0]
            cf = m.groups()[1]
            ver = m.groups()[2]
            gen = m.groups()[3]
            comp = m.groups()[4]
            return SSTableFileName(ks, cf, ver, gen, comp)
        else:
            # Check if it is a latest version >= 3.0
            m = re.compile(r'(.*)-(.*)-(.*)-(.*).db').match(name)
            if m != None:
                ver = m.groups()[0]
                gen = m.groups()[1]
                fmt = m.groups()[2]
                comp = m.groups()[3]
                return SSTableFileName(None, None, ver, gen, comp)
        return None
    parse = classmethod(parse)

    def __repr__(self):
        return "keyspace: %s columnfamily: %s version: %s generation: %s component: %s" % (self.keyspace, self.columnfamily, self.version, self.generation, self.component)

