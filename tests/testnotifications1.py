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
"""This module is a basic test for notifications."""

from testfwk import AffinityTest
from affinity import *
from afynotifier import *
import time

def _entryPoint():
    lDbConn = AffinityConnection()
    lDbConn.open()
    AFYNOTIFIER.open(lDbConn)
    with lDbConn:
        _testLogic()
    AFYNOTIFIER.close()
    lDbConn.close()
    
def _testLogic():
    print ("1a. create a few pins")
    lNewPins = PIN.savePINs([
        PIN({"http://localhost/afy/property/testnotifications1/name":"Sabrina", "http://localhost/afy/property/testnotifications1/functions":"dancer", "http://localhost/afy/property/testnotifications1/age":32}), 
        PIN({"http://localhost/afy/property/testnotifications1/name":"Sophia", "http://localhost/afy/property/testnotifications1/functions":["artist", "scientist"], "http://localhost/afy/property/testnotifications1/age":99}),
        PIN({"http://localhost/afy/property/testnotifications1/name":"Allan", "http://localhost/afy/property/testnotifications1/age":45})])

    print ("1a. create a few classes")
    try:
        AffinityConnection.getCurrentDbConnection().q("CREATE CLASS \"http://localhost/afy/class/testnotifications1/Named\" AS SELECT * WHERE \"http://localhost/afy/property/testnotifications1/name\" IN :0;")
        AffinityConnection.getCurrentDbConnection().q("CREATE CLASS \"http://localhost/afy/class/testnotifications1/Aged\" AS SELECT * WHERE \"http://localhost/afy/property/testnotifications1/age\" IN :0;")
    except:
        pass

    lNumPINNotifs = [0]
    lNumClassNotifs = [0]
    class CheckCounts(object):
        "Helper to track the evolution of lNumPINNotifs and lNumClassNotifs."
        def __init__(self, pExpectedPINInc, pExpectedClassInc):
            self.mExpectedPINInc = pExpectedPINInc
            self.mExpectedClassInc = pExpectedClassInc
        def __enter__(self):
            self.mInitPINVal = lNumPINNotifs[0]
            self.mInitClassVal = lNumClassNotifs[0]
        def __exit__(self, etyp, einst, etb):
            if lNumPINNotifs[0] != (self.mInitPINVal + self.mExpectedPINInc):
                print (" ** pin notif: expected count %d but obtained %d **" % (self.mInitPINVal + self.mExpectedPINInc, lNumPINNotifs[0]))
                assert False
            if lNumClassNotifs[0] != (self.mInitClassVal + self.mExpectedClassInc):
                print (" ** class notif: expected count %d but obtained %d **" % (self.mInitClassVal + self.mExpectedClassInc, lNumClassNotifs[0]))
                assert False

    def onPINNotif(pData, pCriterion):
        print (" -> received PIN notification '%s' (%ss after registration)" % (pCriterion, time.time() - pData))
        lNumPINNotifs[0] = lNumPINNotifs[0] + 1
    def onClassNotif(pData, pCriterion):
        print (" -> received class notification '%s' (%ss after registration)" % (pCriterion, time.time() - pData))
        lNumClassNotifs[0] = lNumClassNotifs[0] + 1

    # Basic test for PIN notifications.
    if True:
        print ("2. register for notifications on pin '%s'" % lNewPins[0].mPID)
        AFYNOTIFIER.registerPIN(lNewPins[0].mPID.mLocalPID, onPINNotif, pHandlerData=time.time(), pGroupNotifs=True)
        time.sleep(1)
        with CheckCounts(0, 0):
            print ("3. change pin %s" % lNewPins[1].mPID)
            lNewPins[1]["http://localhost/afy/property/testnotifications1/name"] = "Steve"
            time.sleep(1)
        with CheckCounts(1, 0):
            print ("4. change pin %s" % lNewPins[0].mPID)
            lNewPins[0]["http://localhost/afy/property/testnotifications1/name"] = "Fred"
            time.sleep(1)
        print ("5. unregister")
        AFYNOTIFIER.unregisterPIN(lNewPins[0].mPID.mLocalPID, onPINNotif)
        time.sleep(1)
        with CheckCounts(0, 0):
            print ("5b. change pin %s after unregistration" % lNewPins[0].mPID)
            lNewPins[0]["http://localhost/afy/property/testnotifications1/name"] = "Freddy"
            time.sleep(1)

    # Basic test for class notifications.
    if True:
        print ("6. register for notifications on class 'http://localhost/afy/class/testnotifications1/Named'")
        AFYNOTIFIER.registerClass("http://localhost/afy/class/testnotifications1/Named", onClassNotif, pHandlerData=time.time(), pGroupNotifs=True)
        time.sleep(1)
        with CheckCounts(0, 1):
            print ("7. change related pin %s" % lNewPins[1].mPID)
            lNewPins[1]["http://localhost/afy/property/testnotifications1/name"] = "Stevee"
            time.sleep(1)
        with CheckCounts(0, 1):
            print ("8a. create related pin")
            PIN({"http://localhost/afy/property/testnotifications1/name":"Sandra", "http://localhost/afy/property/testnotifications1/functions":"musician", "http://localhost/afy/property/testnotifications1/age":31}).savePIN()
            time.sleep(1)
        with CheckCounts(0, 0):
            print ("8b. create unrelated pin")
            PIN({"http://localhost/afy/property/testnotifications1/shape":"square"}).savePIN()
            time.sleep(1)
        print ("9. unregister")
        AFYNOTIFIER.unregisterClass("http://localhost/afy/class/testnotifications1/Named", onClassNotif)
        time.sleep(1)
        with CheckCounts(0, 0):
            print ("9b. change pin %s after unregistration" % lNewPins[1].mPID)
            lNewPins[1]["http://localhost/afy/property/testnotifications1/name"] = "Stephen"
            time.sleep(1)

    # Basic test for mixed notifications.
    if True:
        print ("10. register for notifications on pin '%s' and class 'http://localhost/afy/class/testnotifications1/Named'" % lNewPins[0].mPID)
        AFYNOTIFIER.registerPIN(lNewPins[0].mPID.mLocalPID, onPINNotif, pHandlerData=time.time(), pGroupNotifs=True)
        AFYNOTIFIER.registerClass("http://localhost/afy/class/testnotifications1/Named", onClassNotif, pHandlerData=time.time(), pGroupNotifs=True)
        time.sleep(1)
        with CheckCounts(0, 1):
            print ("11. change semi-related pin %s" % lNewPins[1].mPID)
            lNewPins[1]["http://localhost/afy/property/testnotifications1/name"] = "Roger"
            time.sleep(1)
        with CheckCounts(1, 1):
            print ("12. change related pin %s" % lNewPins[0].mPID)
            lNewPins[0]["http://localhost/afy/property/testnotifications1/name"] = "Anthony"
            time.sleep(1)
        print ("13. unregister")
        AFYNOTIFIER.unregisterClass("http://localhost/afy/class/testnotifications1/Named", onClassNotif)
        AFYNOTIFIER.unregisterPIN(lNewPins[0].mPID.mLocalPID, onPINNotif)
        time.sleep(1)
        with CheckCounts(0, 0):
            print ("13b. change pin %s after unregistration" % lNewPins[0].mPID)
            lNewPins[0]["http://localhost/afy/property/testnotifications1/name"] = "Antoine"
            time.sleep(1)
    
    # Basic test for timeouts.
    if True:
        print ("14. register for notifications on class 'http://localhost/afy/class/testnotifications1/Named'")
        AFYNOTIFIER.registerClass("http://localhost/afy/class/testnotifications1/Named", onClassNotif, pHandlerData=time.time(), pGroupNotifs=True)
        print (" sleeping 10 seconds")
        time.sleep(10)
        with CheckCounts(0, 1):
            print ("15. change related pin %s" % lNewPins[1].mPID)
            lNewPins[1]["http://localhost/afy/property/testnotifications1/name"] = "Pablo"
            print (" sleeping 20 seconds")
            time.sleep(20)
        with CheckCounts(0, 1):
            print ("16. change related pin %s" % lNewPins[1].mPID)
            lNewPins[1]["http://localhost/afy/property/testnotifications1/name"] = "Paolo"
            time.sleep(1)
        print ("17. unregister")
        AFYNOTIFIER.unregisterClass("http://localhost/afy/class/testnotifications1/Named", onClassNotif)

class TestNotifications1(AffinityTest):
    "A basic test for notifications."
    def execute(self):
        _entryPoint()
AffinityTest.declare(TestNotifications1)

if __name__ == '__main__':
    lT = TestNotifications1()
    lT.execute()
