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


import os
import re
import sys
import socket
import struct
import binascii
import lz4

debug = 0

LIVE_MASK            = 0x00
DELETION_MASK        = 0x01
EXPIRATION_MASK      = 0x02
COUNTER_MASK         = 0x04
COUNTER_UPDATE_MASK  = 0x08
RANGE_TOMBSTONE_MASK = 0x10

class Buffer:
    def __init__(self, buf):
        self.buf = buf
        self.offset = 0
        self.buflen = len(buf)
        if (debug):
            print "buflen: %d" % (self.buflen)

    def readbytes(self, count):
        if self.remaining() >= count:
            return
        self.rebuffer()

    def rebuffer(self):
        if (debug):
            print "offset: ", self.offset
        raise NotImplementedError("Not Implemented")
        
    def unpack_int(self):
        int_size = struct.calcsize('i')
        self.readbytes(int_size)
        value = struct.unpack('>i', self.buf[self.offset:self.offset+int_size])[0]
        self.offset += int_size
        return value

    def unpack_short(self):
        short_size = struct.calcsize('h')
        self.readbytes(short_size)
        value = struct.unpack('>h', self.buf[self.offset:self.offset+short_size])[0]
        self.offset += short_size
        return value

    def unpack_byte(self):
        byte_size = struct.calcsize('B')
        self.readbytes(byte_size)
        value = struct.unpack('>B', self.buf[self.offset:self.offset+byte_size])[0]
        self.offset += byte_size
        return value

    def unpack_utf_string(self):
        length = self.unpack_short()
        self.readbytes(length)
        if length == 0:
            return None
        format = '%ds' % length
        value = struct.unpack(format, self.buf[self.offset:self.offset+length])[0]
        self.offset += length
        return value

    def unpack_longlong(self):
        longlong_size = struct.calcsize('Q')
        self.readbytes(longlong_size)
        value = struct.unpack('>Q', self.buf[self.offset:self.offset+longlong_size])[0]
        self.offset += longlong_size
        return value

    def unpack_data(self):
        length = self.unpack_int()
        if length > 0:
            self.readbytes(length)
            format = '%ds' % length
            value = struct.unpack(format, self.buf[self.offset:self.offset+length])[0]
            self.offset += length
            return value
        return None

    def skip_data(self):
        length = self.unpack_int()
        if length > 0:
            self.offset += length

    def get_remaining(self):
        return self.buf[self.offset:]

    def remaining(self):
        return self.buflen - self.offset

class CompressedBuffer(Buffer):
    def __init__(self, datafile, compfile):
        self.compmetadata = CompresssionInfo.parse(compfile)
        if (debug):
            print self.compmetadata
        self.datasize = os.stat(datafile).st_size
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
        if (debug):
            print "row-pos: %d chunklen: %d skipchunkcount: %d seekpos: %d skipbytes: %d" % (pos, self.compmetadata.chunklen, skipchunkcount, seekpos, skipbytes)
        self.file.seek(seekpos)
        self.buf = None
        self.offset = 0
        self.buflen = 0
        self.chunkno = skipchunkcount
        self.rebuffer()
        self.offset = skipbytes

    def rebuffer(self):
        self.buf = self.nextchunk()
        assert self.buf != None
        self.offset = 0
        self.buflen = len(self.buf)
        if (debug):
            print "buflen: %d" % (self.buflen)

    def nextchunk(self):
        if self.chunkno >= self.compmetadata.chunkcount:
            return
        if (self.chunkno + 1 < self.compmetadata.chunkcount):
            chunklen = self.compmetadata.chunkoffsets[self.chunkno + 1] - self.compmetadata.chunkoffsets[self.chunkno]
            chunk = self.file.read(chunklen)
        else:
            chunk = self.file.read()
        if (debug):
            print "chunklen: ", len(chunk)
        self.chunkno += 1
        newbuf = bytearray('')
        if self.remaining() > 0:
            if (debug):
                print "remaining: %d remaining data: %s" % (self.remaining(), self.get_remaining())
            b1 = self.get_remaining()
            newbuf.extend(b1)
        b = self.uncompress_chunk(chunk)
        if (debug):
            print "uncompressed chunklen: ", len(b)
        newbuf.extend(b)
        #newbuf = self.uncompress_chunk(chunk)
        return str(newbuf)

    def uncompress_chunk(self, compressed):
        # skip checksum
        data = compressed[0:len(compressed)-4]
        uncompressed = lz4.loads(data)
        return uncompressed

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
        if (debug):
            print "entries: ", entries
        return IndexInfo(entries)
    parse = classmethod(parse)

class CompresssionInfo:
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
        return CompresssionInfo(classname, paramcount, params, uncompressedlen, chunklen, chunkcount, offsets)
    parse = classmethod(parse)

    def __repr__(self):
        return "class: %s paramcount: %d chunklen: %d uncompressedlen: %d chunkcount: %d" % (self.classname, self.paramcount, self.chunklen, self.uncompressedlen, self.chunkcount)

class SSTableReader:
    def __init__(self, indexfile, datafile, compfile):
        self.index = IndexInfo.parse(indexfile)
        self.buf = CompressedBuffer(datafile, compfile)

    def readrows(self, searchkey):
        if searchkey != "":
            for i in xrange(self.index.rowcount):
                if self.index.entries[i][0].find(searchkey) != -1:
                    print self.index.entries[i]
                    if i + 1 < self.index.rowcount:
                        rowsize = self.index.entries[i + 1][1] - self.index.entries[i][1]
                    else:
                        rowsize = self.buf.datasize
                    self.buf.seek(self.index.entries[i][1])
                    self.readrow(rowsize)
        else:
            for i in xrange(self.index.rowcount):
                if i + 1 < self.index.rowcount:
                    rowsize = self.index.entries[i + 1][1] - self.index.entries[i][1]
                else:
                    rowsize = self.buf.datasize
                self.readrow(rowsize)

    def readrow(self, rowsize):
        key = self.buf.unpack_utf_string()
        print "row key: %s" % (key)
        deletiontime = self.unpack_deletion_time()
        print "localDeletionTime: %x markedForDeleteAt: %x" % (deletiontime[0], deletiontime[1])
        off = 0
        columncount = 0
        livecolumns = 0
        while True:
            name = self.unpack_column_name()
            if name != None:
                columncount += 1
                value = self.unpack_column_value()
                if (value[0] == 0):
                    livecolumns += 1
                print "column name: %s mask: %x ts: %d value: %s" % (name, value[0], value[1], binascii.hexlify(value[2]))
            else:
                break
        print "columncount: %d livecolumns: %d" % (columncount, livecolumns)

    def unpack_deletion_time(self):
        localDeletionTime = self.buf.unpack_int()
        markedForDeleteAt = self.buf.unpack_longlong()
        return (localDeletionTime, markedForDeleteAt)

    def unpack_column_name(self):
        name = self.buf.unpack_utf_string()
        return name

    def unpack_column_value(self):
        flag = self.buf.unpack_byte()
        if (flag & RANGE_TOMBSTONE_MASK) != 0:
            name = self.buf.unpack_utf_string()
            deletiontime = self.unpack_deletion_time()
            return (RANGE_TOMBSTONE_MASK, deletiontime, name)
        else:
            if ((flag & COUNTER_MASK) != 0):
                timestampOfLastDelete = self.buf.unpack_longlong()
                ts = self.buf.unpack_longlong()
                value = self.buf.unpack_data()
                return (COUNTER_MASK, ts, timestampOfLastDelete, value)
            elif (flag & EXPIRATION_MASK) != 0:
                ttl = self.buf.unpack_int()
                expiration = self.buf.unpack_int()
                ts = self.buf.unpack_longlong()
                value = self.buf.unpack_data()
                return (EXPIRATION_MASK, ts, ttl, expiration)
            else:
                ts = self.buf.unpack_longlong()
                value = self.buf.unpack_data()
                if (flag & COUNTER_UPDATE_MASK) != 0:
                    return (COUNTER_UPDATE_MASK, value, ts)
                elif (flag & DELETION_MASK) != 0:
                    return (DELETION_MASK, ts, value)
                else:
                    return (LIVE_MASK, ts, value)

rowkey = ""
if len(sys.argv) < 4:
    print "Usage: python sstable <index file> <data file> <compression file> [row key]"
    sys.exit(1)

if len(sys.argv) > 4:
    rowkey = sys.argv[4]
SSTableReader(sys.argv[1], sys.argv[2], sys.argv[3]).readrows(rowkey)
