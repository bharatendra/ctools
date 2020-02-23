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

import struct
from datetime import datetime
import uuid

debug = 0
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
        if (debug):
            print "count: ",count
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

    def unpack_signed_byte(self):
        byte_size = struct.calcsize('b')
        self.readbytes(byte_size)
        value = struct.unpack('>b', self.buf[self.offset:self.offset+byte_size])[0]
        self.offset += byte_size
        return value

    def unpack_utf_string(self):
        length = self.unpack_short()
        if length == 0:
            return ""
        if (debug):
            print "length: %d" % (length)
        self.readbytes(length)
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

    def unpack_float(self):
        float_size = struct.calcsize('f')
        self.readbytes(float_size)
        value = struct.unpack('>f', self.buf[self.offset:self.offset+float_size])[0]
        self.offset += float_size
        return value

    def unpack_double(self):
        double_size = struct.calcsize('d')
        self.readbytes(double_size)
        value = struct.unpack('>d', self.buf[self.offset:self.offset+double_size])[0]
        self.offset += double_size
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

    def unpack_bytes(self, length):
        if length > 0:
            self.readbytes(length)
            format = '%ds' % length
            value = struct.unpack(format, self.buf[self.offset:self.offset+length])[0]
            self.offset += length
            return value
        return None

    def unpack_date(self):
        length = self.unpack_short()
        if length == 0:
            return ""
        ts = self.unpack_longlong()
        date = datetime.fromtimestamp(ts/1000)
        s = date.strftime("%Y-%m-%d %H:%M")
        r = s.replace(":", '\\\\:')
        return r

    def unpack_uuid(self):
        length = self.unpack_short()
        if length == 0:
            return ""
        self.readbytes(length)
        format = '%ds' % length
        value = struct.unpack(format, self.buf[self.offset:self.offset+length])[0]
        self.offset += length
        x = uuid.UUID(bytes=value)
        return str(x)

    def unpack_boolean(self):
        length = self.unpack_short()
        if length == 0:
            return ""
        self.readbytes(length)
        byte = self.unpack_byte()
        if byte == 0:
            return "false"
        return "true"

    def unpack_vint(self):
        byte = self.unpack_signed_byte()
        # MSB bit test
        if byte & 0x80 != 0x80:
            return byte
        # Get number of extra bytes to read
        mask = 0x80
        extrabytes = 0
        while byte & mask != 0:            
            extrabytes = extrabytes + 1
            mask = mask >> 1
        mask = 0x80
        i = 0
        while i < extrabytes - 1:
            mask = mask >> 1
            mask = mask | 0x80
            i = i + 1
        mask = (~mask & 0xff)
        val = (byte & mask)
        i = 0
        while i < extrabytes:
            val = val << 8
            byte = self.unpack_signed_byte()
            val = val | (byte & 0xff)
            i = i + 1
        return val

    def unpack_vintlendata(self):
        length = self.unpack_vint()
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

    def skip_bytes(self, length):
        if length > 0:
            self.offset += length

    def get_remaining(self):
        return self.buf[self.offset:]

    def remaining(self):
        return self.buflen - self.offset

    def available(self):
        return (self.remaining() > 0)

    def seek(self, off):
        self.offset = off
