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


# a script to convert a given key in hex format to token using RandomPartitioner

import hashlib
import binascii
import sys

if len(sys.argv) < 2:
    print "usage: python token.py <key in hex format>"
    sys.exit(1)

key = binascii.unhexlify(sys.argv[1])

# Calculate MD5 digest and convert it to hex format
digest = hashlib.md5(key).hexdigest()

# Convert the hash digest to 2's complement form
token  = long(digest, 16)
bits   = 128
if ((token & (1 << (bits - 1))) != 0):
    token = token - (1 << bits)

# Convert the resulting number to unsigned form
print abs(token)
