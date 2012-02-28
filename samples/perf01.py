#!/usr/bin/env python2.6
# Copyright (c) 2004-2012 VMware, Inc. All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,  WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
# -----
"""This module contains basic performance assessments (in sync with the nodejs file of the same name)."""

from affinity import *
import datetime
import random
import string
import time

def randomString(pLen):
    return ''.join(random.choice(string.letters) for i in xrange(pLen))

if __name__ == '__main__':
    NUM_PINS = 50
    KEEP_ALIVE = True
    lAffinity = AFFINITY()
    lAffinity.open(pKeepAlive=KEEP_ALIVE)
    
    # Bootstrap.
    lT1 = time.time()
    lAffinity.startTx()
    PIN({"http://localhost/afy/property/perf01/startedAt":datetime.datetime.now()}).savePIN()
    lAffinity.commitTx()
    print ("bootstrap: %s s" % (time.time() - lT1))

    # pathSQL.
    lT1 = time.time()
    if KEEP_ALIVE:
        lAffinity.q("SET PREFIX perf01p: 'http://localhost/afy/property/perf01/';")
        lAffinity.q("START TRANSACTION;")
        for iP in xrange(NUM_PINS):
            lAffinity.q("INSERT (perf01p:name, perf01p:code, perf01p:type) VALUES ('" + randomString(10) + "', '" + randomString(15) + "', 'pathsql');")
        lAffinity.q("COMMIT;")
    else:
        for iP in xrange(NUM_PINS):
            lAffinity.q("INSERT (\"http://localhost/afy/property/perf01/name\", \"http://localhost/afy/property/perf01/code\", \"http://localhost/afy/property/perf01/type\") VALUES ('" + randomString(10) + "', '" + randomString(15) + "', 'pathsql');")
    print ("pathsql: %s s" % (time.time() - lT1))

    # protobuf.
    lT1 = time.time()
    lAffinity.startTx()
    for iP in xrange(NUM_PINS):
        PIN({"http://localhost/afy/property/perf01/name":randomString(10), "http://localhost/afy/property/perf01/code":randomString(15), "http://localhost/afy/property/perf01/type":"protobuf"}).savePIN()
    lAffinity.commitTx()
    print ("protobuf: %s s" % (time.time() - lT1))    

    lAffinity.close()
