#!/usr/bin/env python2.6
"""This module is a basic test for notifications."""

from testfwk import MVStoreTest
from mvstore import *
from mvnotifier import *
import time

def _entryPoint():
    lDbConn = MVStoreConnection()
    lDbConn.open()
    MVNOTIFIER.open(lDbConn)
    with lDbConn:
        _testLogic()
    MVNOTIFIER.close()
    lDbConn.close()
    
def _testLogic():
    print ("1a. create a few pins")
    lNewPins = PIN.savePINs([
        PIN({"http://localhost/mv/property/testnotifications1/name":"Sabrina", "http://localhost/mv/property/testnotifications1/functions":"dancer", "http://localhost/mv/property/testnotifications1/age":32}), 
        PIN({"http://localhost/mv/property/testnotifications1/name":"Sophia", "http://localhost/mv/property/testnotifications1/functions":["artist", "scientist"], "http://localhost/mv/property/testnotifications1/age":99}),
        PIN({"http://localhost/mv/property/testnotifications1/name":"Allan", "http://localhost/mv/property/testnotifications1/age":45})])

    print ("1a. create a few classes")
    try:
        MVStoreConnection.getCurrentDbConnection().q("CREATE CLASS \"http://localhost/mv/class/testnotifications1/Named\" AS SELECT * WHERE \"http://localhost/mv/property/testnotifications1/name\" IN :0;")
        MVStoreConnection.getCurrentDbConnection().q("CREATE CLASS \"http://localhost/mv/class/testnotifications1/Aged\" AS SELECT * WHERE \"http://localhost/mv/property/testnotifications1/age\" IN :0;")
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
        MVNOTIFIER.registerPIN(lNewPins[0].mPID.mLocalPID, onPINNotif, pHandlerData=time.time(), pGroupNotifs=True)
        time.sleep(1)
        with CheckCounts(0, 0):
            print ("3. change pin %s" % lNewPins[1].mPID)
            lNewPins[1]["http://localhost/mv/property/testnotifications1/name"] = "Steve"
            time.sleep(1)
        with CheckCounts(1, 0):
            print ("4. change pin %s" % lNewPins[0].mPID)
            lNewPins[0]["http://localhost/mv/property/testnotifications1/name"] = "Fred"
            time.sleep(1)
        print ("5. unregister")
        MVNOTIFIER.unregisterPIN(lNewPins[0].mPID.mLocalPID, onPINNotif)
        time.sleep(1)
        with CheckCounts(0, 0):
            print ("5b. change pin %s after unregistration" % lNewPins[0].mPID)
            lNewPins[0]["http://localhost/mv/property/testnotifications1/name"] = "Freddy"
            time.sleep(1)

    # Basic test for class notifications.
    if True:
        print ("6. register for notifications on class 'http://localhost/mv/class/testnotifications1/Named'")
        MVNOTIFIER.registerClass("http://localhost/mv/class/testnotifications1/Named", onClassNotif, pHandlerData=time.time(), pGroupNotifs=True)
        time.sleep(1)
        with CheckCounts(0, 1):
            print ("7. change related pin %s" % lNewPins[1].mPID)
            lNewPins[1]["http://localhost/mv/property/testnotifications1/name"] = "Stevee"
            time.sleep(1)
        with CheckCounts(0, 1):
            print ("8a. create related pin")
            PIN({"http://localhost/mv/property/testnotifications1/name":"Sandra", "http://localhost/mv/property/testnotifications1/functions":"musician", "http://localhost/mv/property/testnotifications1/age":31}).savePIN()
            time.sleep(1)
        with CheckCounts(0, 0):
            print ("8b. create unrelated pin")
            PIN({"http://localhost/mv/property/testnotifications1/shape":"square"}).savePIN()
            time.sleep(1)
        print ("9. unregister")
        MVNOTIFIER.unregisterClass("http://localhost/mv/class/testnotifications1/Named", onClassNotif)
        time.sleep(1)
        with CheckCounts(0, 0):
            print ("9b. change pin %s after unregistration" % lNewPins[1].mPID)
            lNewPins[1]["http://localhost/mv/property/testnotifications1/name"] = "Stephen"
            time.sleep(1)

    # Basic test for mixed notifications.
    if True:
        print ("10. register for notifications on pin '%s' and class 'http://localhost/mv/class/testnotifications1/Named'" % lNewPins[0].mPID)
        MVNOTIFIER.registerPIN(lNewPins[0].mPID.mLocalPID, onPINNotif, pHandlerData=time.time(), pGroupNotifs=True)
        MVNOTIFIER.registerClass("http://localhost/mv/class/testnotifications1/Named", onClassNotif, pHandlerData=time.time(), pGroupNotifs=True)
        time.sleep(1)
        with CheckCounts(0, 1):
            print ("11. change semi-related pin %s" % lNewPins[1].mPID)
            lNewPins[1]["http://localhost/mv/property/testnotifications1/name"] = "Roger"
            time.sleep(1)
        with CheckCounts(1, 1):
            print ("12. change related pin %s" % lNewPins[0].mPID)
            lNewPins[0]["http://localhost/mv/property/testnotifications1/name"] = "Anthony"
            time.sleep(1)
        print ("13. unregister")
        MVNOTIFIER.unregisterClass("http://localhost/mv/class/testnotifications1/Named", onClassNotif)
        MVNOTIFIER.unregisterPIN(lNewPins[0].mPID.mLocalPID, onPINNotif)
        time.sleep(1)
        with CheckCounts(0, 0):
            print ("13b. change pin %s after unregistration" % lNewPins[0].mPID)
            lNewPins[0]["http://localhost/mv/property/testnotifications1/name"] = "Antoine"
            time.sleep(1)
    
    # Basic test for timeouts.
    if True:
        print ("14. register for notifications on class 'http://localhost/mv/class/testnotifications1/Named'")
        MVNOTIFIER.registerClass("http://localhost/mv/class/testnotifications1/Named", onClassNotif, pHandlerData=time.time(), pGroupNotifs=True)
        print (" sleeping 10 seconds")
        time.sleep(10)
        with CheckCounts(0, 1):
            print ("15. change related pin %s" % lNewPins[1].mPID)
            lNewPins[1]["http://localhost/mv/property/testnotifications1/name"] = "Pablo"
            print (" sleeping 20 seconds")
            time.sleep(20)
        with CheckCounts(0, 1):
            print ("16. change related pin %s" % lNewPins[1].mPID)
            lNewPins[1]["http://localhost/mv/property/testnotifications1/name"] = "Paolo"
            time.sleep(1)
        print ("17. unregister")
        MVNOTIFIER.unregisterClass("http://localhost/mv/class/testnotifications1/Named", onClassNotif)

class TestNotifications1(MVStoreTest):
    "A basic test for notifications."
    def execute(self):
        _entryPoint()
MVStoreTest.declare(TestNotifications1)

if __name__ == '__main__':
    lT = TestNotifications1()
    lT.execute()
