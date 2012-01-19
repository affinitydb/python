#!/usr/bin/env python2.6
"""This module contains basic performance assessments (in sync with the nodejs file of the same name)."""

from mvstore import *
import datetime
import random
import string
import time

def randomString(pLen):
    return ''.join(random.choice(string.letters) for i in xrange(pLen))

if __name__ == '__main__':
    NUM_PINS = 50
    KEEP_ALIVE = True
    lMvStore = MVSTORE()
    lMvStore.open(pKeepAlive=KEEP_ALIVE)
    
    # Bootstrap.
    lT1 = time.time()
    lMvStore.startTx()
    PIN({"http://localhost/mv/property/perf01/startedAt":datetime.datetime.now()}).savePIN()
    lMvStore.commitTx()
    print ("bootstrap: %s s" % (time.time() - lT1))

    # pathSQL.
    lT1 = time.time()
    if KEEP_ALIVE:
        lMvStore.q("SET PREFIX perf01p: 'http://localhost/mv/property/perf01/';")
        lMvStore.q("START TRANSACTION;")
        for iP in xrange(NUM_PINS):
            lMvStore.q("INSERT (perf01p:name, perf01p:code, perf01p:type) VALUES ('" + randomString(10) + "', '" + randomString(15) + "', 'pathsql');")
        lMvStore.q("COMMIT;")
    else:
        for iP in xrange(NUM_PINS):
            lMvStore.q("INSERT (\"http://localhost/mv/property/perf01/name\", \"http://localhost/mv/property/perf01/code\", \"http://localhost/mv/property/perf01/type\") VALUES ('" + randomString(10) + "', '" + randomString(15) + "', 'pathsql');")
    print ("pathsql: %s s" % (time.time() - lT1))

    # protobuf.
    lT1 = time.time()
    lMvStore.startTx()
    for iP in xrange(NUM_PINS):
        PIN({"http://localhost/mv/property/perf01/name":randomString(10), "http://localhost/mv/property/perf01/code":randomString(15), "http://localhost/mv/property/perf01/type":"protobuf"}).savePIN()
    lMvStore.commitTx()
    print ("protobuf: %s s" % (time.time() - lT1))    

    lMvStore.close()
