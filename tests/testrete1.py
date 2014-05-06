#!/usr/bin/env python2.6
# Copyright (c) 2004-2014 GoPivotal, Inc. All Rights Reserved.
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
"""This module is a basic test to compare the performance overhead of
a very large number of classes in a flat organization vs tree organization."""

from testfwk import AffinityTest
from affinity import *
import random
import time

TESTRETE1_NUMCLASSES=100
TESTRETE1_NUMCHECKS=10000

def _entryPoint():
    _applyTest(AffinityConnection("localhost", 4560, "rete1_flat"), _testFlat)
    _applyTest(AffinityConnection("localhost", 4560, "rete1_hier"), _testHierarchical)

def _applyTest(pDbConn, pClass):
    pDbConn.open()
    lT1 = time.time()
    lC = pClass(pDbConn)
    lT2 = time.time()
    print ("class %s initialized in %s s" % (pClass, lT2 - lT1))
    with pDbConn:
        lC.run()
    print ("class %s ran in %s s" % (pClass, time.time() - lT2))
    print ("class %s retrieved %d PINs" % (pClass, lC.check()))
    pDbConn.close()

class _testFlat:
    def __init__(self, pDbConn):
        self.mDbConn = pDbConn
        for iC in xrange(TESTRETE1_NUMCLASSES):
            self.mDbConn.q("CREATE CLASS \"http://testrete1/flat/c%d\" AS SELECT * WHERE \"http://testrete1/flat/v\"=%d" % (iC, iC))
    def run(self):
        for iC in xrange(TESTRETE1_NUMCHECKS):
            self.mDbConn.q("INSERT \"http://testrete1/flat/v\"=%d" % (random.randrange(0, TESTRETE1_NUMCLASSES)))
    def check(self):
        lTot = 0
        for iC in xrange(TESTRETE1_NUMCLASSES):
            lTot += self.mDbConn.qCount("SELECT * FROM \"http://testrete1/flat/c%d\"" % iC)
        return lTot

class _testHierarchical:
    def __init__(self, pDbConn):
        self.mDbConn = pDbConn
        self.createClasses(0, TESTRETE1_NUMCLASSES, "*")
    def createClasses(self, pFrom, pTo, pBase):
        if pFrom < 0 or pTo < pFrom:
            return
        elif pFrom == pTo:
            self.mDbConn.q("CREATE CLASS \"http://testrete1/hier/c%d\" AS SELECT * FROM \"%s\" WHERE \"http://testrete1/hier/v\"=%d" % (pFrom, pBase, pFrom))
        else:
            lMid = (pFrom + pTo) / 2
            # left
            if lMid > pFrom:
                lLeftCn = "http://testrete1/hier/cb%dto%d" % (pFrom, lMid)
                self.mDbConn.q("CREATE CLASS \"%s\" AS SELECT * FROM \"%s\" WHERE \"http://testrete1/hier/v\" >= %d AND \"http://testrete1/hier/v\" <= %d" % (lLeftCn, pBase, pFrom, lMid))
            else:
                lLeftCn = pBase                
            self.createClasses(pFrom, lMid, lLeftCn)
            # right
            if lMid + 1 < pTo:
                lRightCn = "http://testrete1/hier/cb%dto%d" % (lMid + 1, pTo)
                self.mDbConn.q("CREATE CLASS \"%s\" AS SELECT * FROM \"%s\" WHERE \"http://testrete1/hier/v\" >= %d AND \"http://testrete1/hier/v\" <= %d" % (lRightCn, pBase, lMid + 1, pTo))
            else:
                lRightCn = pBase
            self.createClasses(lMid + 1, pTo, lRightCn)
    def run(self):
        for iC in xrange(TESTRETE1_NUMCHECKS):
            self.mDbConn.q("INSERT \"http://testrete1/hier/v\"=%d" % (random.randrange(0, TESTRETE1_NUMCLASSES)))
    def check(self):
        lTot = 0
        for iC in xrange(TESTRETE1_NUMCLASSES):
            lTot += self.mDbConn.qCount("SELECT * FROM \"http://testrete1/hier/c%d\"" % iC)
        return lTot

class TestRete1(AffinityTest):
    "A basic test for flat vs hierarchical classification."
    def execute(self):
        _entryPoint()
AffinityTest.declare(TestRete1)

if __name__ == '__main__':
    lT = TestRete1()
    lT.execute()
