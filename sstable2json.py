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
import binascii
import argparse
from sstable import *

def export(reader):
    firstrow = True
    print "["
    while reader.hasnext():
        if firstrow == True:
            firstrow = False
        else:
            sys.stdout.write(",\n")
        row = reader.next()
        sys.stdout.write("{\"key\": \"%s\",\n" % binascii.hexlify(row.key))
        if row.getdeletioninfo().islive() == False:
            sys.stdout.write(" \"metadata\": ")
            sys.stdout.write("{");
            sys.stdout.write("\"deletionInfo\": ")
            sys.stdout.write("{\"markedForDeleteAt\": %d" %  row.getdeletioninfo().markedForDeleteAt)
            sys.stdout.write(", \"localDeletionTime\": %d}" %  row.getdeletioninfo().localDeletionTime)
            sys.stdout.write("}")
            sys.stdout.write(",\n")

        sys.stdout.write(" ");
        firstcol = True
        sys.stdout.write("\"cells\": [")
        while row.hasnextcolumn():
            if firstcol == True:
                firstcol = False
            else:
                sys.stdout.write(",\n\t")
            column = row.nextcolumn()
            if isinstance(column, RangeTombstone):
                sys.stdout.write("[\"%s\",\"%s\",%d,\"t\",%d]" % (column.mincol,column.maxcol,column.deletiontime.markedForDeleteAt,column.deletiontime.localDeletionTime))
            else:
                if isinstance(column, DeletedColumn):
                    sys.stdout.write("[\"%s\",\"%s\",%d,\"d\"]" % (column.name, binascii.hexlify(column.value), column.ts))
                elif isinstance(column, ExpiringColumn):
                    sys.stdout.write("[\"%s\",\"%s\",%d,\"e\",%d,%d]" % (column.name, binascii.hexlify(column.value), column.ts,column.ttl,column.expiration))
                elif isinstance(column, CounterColumn):
                    sys.stdout.write("[\"%s\",\"%s\",%d,\"c\",%d]" % (column.name, binascii.hexlify(column.value), column.ts, column.timestampOfLastDelete))
                else:
                    sys.stdout.write("[\"%s\",\"%s\",%d]" % (column.name, binascii.hexlify(column.value), column.ts))
        sys.stdout.write("]}")
    sys.stdout.write("\n]\n")

parser = argparse.ArgumentParser()
parser.add_argument("-v", "--verbose", help="increase output verbosity", action="store_true")
parser.add_argument("-c", "--composite", help="comma separated column types in composite", type=str)
parser.add_argument("sstable", type=str, help="SSTable Data file")
args = parser.parse_args()

datafile = args.sstable
indexfile = datafile.replace("-Data", "-Index")
compfile = datafile.replace("-Data", "-CompressionInfo")

if os.path.isfile(datafile) != True:
    print "%s not exists" % datafile
    sys.exit(1)

if os.path.isfile(indexfile) != True:
    print "%s not exists" % indexfile
    sys.exit(1)

compressed = True
if os.path.isfile(compfile) != True:
    compressed = False

compositetype = []
if args.columntypes != None:
    compositetype = args.columntypes.split(",")
reader = SSTableReader(indexfile, datafile, compfile, compressed, compositetype)
export(reader)
