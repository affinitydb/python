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

# TODO: test negative array indexes, slices etc. for collections - IN PROGRESS
# TODO: test transitions between scalar and collection - IN PROGRESS
# TODO: test extras when dealing directly with the PIN.Collection...

from testfwk import AffinityTest
from copy import copy
from affinity import *
import random
import time

def reportError(pTxt):
    print ("ERROR: %s" % pTxt)
    assert False

def checkPersistedProp(pTxt, pPin, pProperty, pExpectedValue):
    lPinChk = PIN.createFromPID(pPin.mPID)
    if 0 != cmp(lPinChk[pProperty], pExpectedValue):
        print ("expected value: %s" % pExpectedValue)
        print ("actual value: %s" % lPinChk[pProperty])
        print (lPinChk)
        reportError(pTxt)
    else:
        print ("SUCCESS: %s" % pTxt)

def _entryPoint():
    lAffinity = AFFINITY()
    lAffinity.open()
    
    # Create random collections.
    # Review: If I try to create a collection of 1000 elements in one go, it busts some buffer limits in the server...
    lCollection1 = [''.join(random.choice(string.letters) for i in xrange(random.choice((5, 10, 15, 20)))) for i in xrange(200)]
    lCollection2 = [''.join(random.choice(string.letters) for i in xrange(random.choice((5, 10, 15, 20)))) for i in xrange(10)]
    lCollection3 = (1, 2, 3, 4, 5)

    # Create and persist a PIN containing it.
    lPin1 = PIN({"http://localhost/afy/property/testcollections1/myprop1":"lPin1", "http://localhost/afy/property/testcollections1/mycoll1":lCollection1, \
        "http://localhost/afy/property/testcollection1/mycoll3":lCollection3 # Review: to be removed... \
        }).savePIN()

    # To make 100% sure that there's no interference, clone the seeding collection, as a future reference.
    lCollection1 = copy(lCollection1)

    # Reload the PIN, make sure it has the same collection.
    checkPersistedProp("comparing reloaded testcollections1/mycoll1 to lCollection1 initializer", lPin1, "http://localhost/afy/property/testcollections1/mycoll1", lCollection1)

    # Remove a few elements.
    if True:
        del lCollection1[100:150]
        del lPin1["http://localhost/afy/property/testcollections1/mycoll1"][100:150]
        if 0 != cmp(lPin1["http://localhost/afy/property/testcollections1/mycoll1"], lCollection1):
            reportError("comparing in-memory testcollections1/mycoll1 to lCollection1, after removal of 50 elements")
        checkPersistedProp("comparing reloaded testcollections1/mycoll1 to lCollection1, after removal of 50 elements", lPin1, "http://localhost/afy/property/testcollections1/mycoll1", lCollection1)

    # Insert new elements "by position".
    # Review: currently Affinity doesn't return those eids... waiting for a fix...
    if False:
        print ("inserting: %s" % lCollection2)
        print ("in: %s" % lPin1)
        for iE in lCollection2:
            # Interestingly, for something like this to work, immediate updates are required (after loop 0, the insertion point is always the previously inserted eid).
            lCollection1.insert(2, iE) #50, iE)
            lPin1["http://localhost/afy/property/testcollections1/mycoll1"].insert(2, iE) #50, iE)
        print ("after: %s" % lPin1)
        if 0 != cmp(lPin1["http://localhost/afy/property/testcollections1/mycoll1"], lCollection1):
            reportError("comparing in-memory testcollections1/mycoll1 to lCollection1, after insert by position")
        checkPersistedProp("comparing reloaded testcollections1/mycoll1 to lCollection1, after insert by position", lPin1, "http://localhost/afy/property/testcollections1/mycoll1", lCollection1)

    # Insert new elements "by eid".
    if True:
        lIndexPivot = 5
        lEidPivot = lPin1.getExtra("http://localhost/afy/property/testcollections1/mycoll1", pEpos=lIndexPivot).mEid
        for iE in lCollection2:
            PIN({PIN.SK_PID:lPin1.mPID, "http://localhost/afy/property/testcollections1/mycoll1":(iE, PIN.Extra(pOp=affinity_pb2.Value.OP_ADD_BEFORE, pEid=lEidPivot))}).savePIN()
            lCollection1.insert(lIndexPivot, iE)
            lIndexPivot = lIndexPivot + 1
        lPin1.refreshPIN()
        if 0 != cmp(lPin1["http://localhost/afy/property/testcollections1/mycoll1"], lCollection1):
            reportError("comparing reloaded testcollections1/mycoll1 to lCollection1, after insert by eid")
        else:
            print ("SUCCESS: comparing reloaded testcollections1/mycoll1 to lCollection1, after insert by eid")

    # Delete every element of a collection.
    # Review: currently, the last element remains represented as a list... apparently, even the store does that... check if true in c++...
    # lPin1["testcollection1/mycoll3"] = lCollection3 # Review: to be reenabled when the store returns eids...
    if True:
        lCollection3b = list(lCollection3)
        checkPersistedProp("comparing reloaded testcollections1/mycoll3 to lCollection3", lPin1, "http://localhost/afy/property/testcollection1/mycoll3", lCollection3b)
        while len(lCollection3b) > 1:
            del lPin1["http://localhost/afy/property/testcollection1/mycoll3"][0]
            del lCollection3b[0]
            if 0 != cmp(lPin1["http://localhost/afy/property/testcollection1/mycoll3"], lCollection3b):
                reportError("comparing in-memory testcollections1/mycoll3 to lCollection3b, after deletion of element")
            checkPersistedProp("comparing reloaded testcollections1/mycoll3 to lCollection3b, after deletion of element", lPin1, "http://localhost/afy/property/testcollection1/mycoll3", lCollection3b)
        del lPin1["http://localhost/afy/property/testcollection1/mycoll3"]
        try:
            if lPin1.has_key("http://localhost/afy/property/testcollection1/mycoll3") or lPin1.getExtra("http://localhost/afy/property/testcollection1/mycoll3"):
                reportError("retrieving testcollection1/mycoll3 after deletion of the property")
        except Exception as ex:
            #print repr(ex)
            pass
        lPinChk = PIN.createFromPID(lPin1.mPID)
        if lPinChk.has_key("http://localhost/afy/property/testcollection/mycoll3"):
            reportError("retrieving testcollection1/mycoll3 after deletion of the property, on reloaded pin")

    lAffinity.close()

class TestCollections1(AffinityTest):
    "A basic test for collections."
    def execute(self):
        _entryPoint()
AffinityTest.declare(TestCollections1)

if __name__ == '__main__':
    lT = TestCollections1()
    lT.execute()
