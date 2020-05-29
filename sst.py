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
from sstmd import SSTableMetadata
from sstable import SSTableFileName
import argparse
import sys


parser = argparse.ArgumentParser(prog="sst")
parser.add_argument("-m", "--metadata", dest='md', help="display SSTable metadata", action="store_true")
parser.add_argument("-v", "--verbose", dest='verbose', help="increase output verbosity", action="store_true")
parser.add_argument("sstable", type=str, help="SSTable file")
args = parser.parse_args()
option = "metadata"
verbose = False
if args.verbose:
    verbose = True
if args.sstable is None:
    print "please specify sstable file"
    sys.exit(1)
filename = args.sstable
if args.sstable.find("-Data.db") != -1:
    filename = args.sstable.replace("-Data.db", "-Statistics.db")
sstable = SSTableFileName.parse(filename, verbose)

if option == "metadata":
    metadata = SSTableMetadata.parse(filename, sstable.version)
    print metadata


