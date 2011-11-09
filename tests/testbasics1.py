#!/usr/bin/env python2.6
from testfwk import MVStoreTest
from mvstore import *
import random
import string
import uuid
import os
import time
import datetime

def comparePins(pP1, pP2):
    # Note:
    #   This method of comparison is not complete; for example
    #   there's no provision to account for float/double imprecisions...
    if pP1 == pP2:
        print ("YEAH! Compared pins were identical, as expected!")
    else:
        print ("WARNING! Compared pins were different:\n%s\n...vs...\n%s" % (pP1, pP2))
        assert False

def _entryPoint():
    lMvStore = MVSTORE()
    lMvStore.open()

    print ("\nA basic transaction, with pin creations (including collections).")
    lMvStore.startTx()
    PIN.savePINs([
        PIN({"http://localhost/mv/property/testbasics1/myprop1":"salut", "http://localhost/mv/property/testbasics1/myprop2":123}), 
        PIN({"http://localhost/mv/property/testbasics1/myprop1":"bonjour", "http://localhost/mv/property/testbasics1/myprop3":456})])
    lPinDef = {"http://localhost/mv/property/testbasics1/myprop1":["comment", "allez", "vous", "donc"], "http://localhost/mv/property/testbasics1/myprop2":123.5, "http://localhost/mv/property/testbasics1/myprop3":True, "http://localhost/mv/property/testbasics1/myprop4":"-65"}
    lPin = PIN(lPinDef).savePIN()
    lMvStore.commitTx()

    print ("\nBasic examples of mvSQL commands.")
    lMvStore.mvsql("insert (\"http://localhost/mv/property/testbasics1/potato_color\") values (10);") # Note: sent directly to the server, as a string.
    lMvStore.mvsqlProto("insert (\"http://localhost/mv/property/testbasics1/potato_color\") values (11);") # Note: sent to the server as a query embedded inside a protobuf.
    lPins = PIN.loadPINs(lMvStore.mvsqlProto("SELECT * WHERE EXISTS(\"http://localhost/mv/property/testbasics1/myprop1\");"))
    print ("result of SELECT * WHERE EXISTS(\"http://localhost/mv/property/testbasics1/myprop1\"): %s" % lPins)
    try:
        lMvStore.mvsqlProto("CREATE CLASS \"http://localhost/mv/class/testbasics1/c1\" AS SELECT * WHERE EXISTS(\"http://localhost/mv/property/testbasics1/myprop1\");") # TODO: check if this really worked (when embedded in protobuf like this)...
    except:
        pass

    print ("\nGet resulting pins.")
    lRaw = lMvStore.mvsql("SELECT * FROM \"http://localhost/mv/class/testbasics1/c1\";")
    lPBStream = mvstore_pb2.MVStream()
    lPBStream.ParseFromString(lRaw)
    if False:
        displayPBStr(lRaw, pTitle="raw result of SELECT * FROM \"http://localhost/mv/class/testbasics1/c1\"")
        print ("result: %s" % repr(lPBStream.pins))

    print ("\nCheck lPinDef.")
    lReadCtx = PBReadCtx(lPBStream)
    for iP in lPBStream.pins:
        if iP.id.id == lPin.mPID.mLocalPID:
            lPinChk = PIN().loadPIN(lReadCtx, iP)
            comparePins(lPin, lPinChk)

    print ("\nBasic collection modification.")
    print (repr(lPin))
    print ("num values in fetched pin: %d" % len(lPin.keys()))
    print ("num values in fetched pin's myprop1 collection: %d" % len(lPin["http://localhost/mv/property/testbasics1/myprop1"]))
    lPin.moveXafterY("http://localhost/mv/property/testbasics1/myprop1", 2, 0)
    print (lPin)
    lPinAfter = PIN.createFromPID(lPin.mPID)
    comparePins(lPin, lPinAfter)
    lPin.refreshPIN()
    
    print ("\nBasic demonstration of in-place modifications (adding myprop5 and modifying myprop2).")
    lPin["http://localhost/mv/property/testbasics1/myprop5"] = "some newly added value"
    lPin["http://localhost/mv/property/testbasics1/myprop2"] = lPin["http://localhost/mv/property/testbasics1/myprop2"] + 50.4321
    del lPin["http://localhost/mv/property/testbasics1/myprop1"][3]
    print (lPin)
    lPinAfter = PIN.createFromPID(lPin.mPID)
    comparePins(lPin, lPinAfter)
    lPin.refreshPIN()

    print ("\nBasic demonstration of OP_PLUS (on myprop4 of pin %s)." % repr(lPin.mPID))
    lValBef = int(lPin["http://localhost/mv/property/testbasics1/myprop4"])
    PIN({"http://localhost/mv/property/testbasics1/myprop4":(-35, PIN.Extra(pType=mvstore_pb2.Value.VT_INT, pOp=mvstore_pb2.Value.OP_PLUS))}, __PID__=lPin.mPID).savePIN()
    lPin.refreshPIN()
    if int(lPin["http://localhost/mv/property/testbasics1/myprop4"]) != lValBef - 35:
        print ("Unexpected value after OP_PLUS: %d (expected %d)" % (int(lPin["http://localhost/mv/property/testbasics1/myprop4"]), lValBef - 35))
        assert False

    if False:
        print ("\nA simple test for batch-insert of inter-connected uncommitted pins.")
        PIN.savePINs([
            PIN({PIN.SK_PID:1, "http://localhost/mv/property/testbasics1/referenced_by":PIN.Ref(2), "http://localhost/mv/property/testbasics1/text":"1 <- 2"}), 
            PIN({PIN.SK_PID:2, "http://localhost/mv/property/testbasics1/references":PIN.Ref(1), "http://localhost/mv/property/testbasics1/text":"2 -> 1"})])

    if len(lPBStream.pins) > 1:
        print ("\nA basic PIN modification test (ISession::modify style).")
        lPid = lPBStream.pins[1].id
        print ("before modify: %s" % lMvStore.check("SELECT * FROM {@%x};" % lPid.id))
        lPin = PIN({PIN.SK_PID:PIN.PID.fromPB(lPid), "http://localhost/mv/property/testbasics1/myprop1":"bien"})
        #lPin = PIN(__PID__=PIN.PID.fromPB(lPid), testbasics1_myprop1="bien")
        lPin.savePIN()
        print ("after modify: %s" % lMvStore.check("SELECT * FROM {@%x};" % lPid.id))

    if len(lPBStream.pins) > 1:
        print ("\nA basic PIN deletion test.")
        lPid = lPBStream.pins[1].id
        print ("before delete: %s" % lMvStore.check("SELECT * FROM {@%x};" % lPid.id))
        PIN.deletePINs([PIN.PID.fromPB(lPid)])
        #print ("after delete: %s" % lMvStore.check("SELECT * FROM {@%x};" % lPid.id))

    #print ("\nFinal state check.")
    #print (lMvStore.check("SELECT *"))
    lMvStore.close()

class TestBasics1(MVStoreTest):
    "First basic test (originally written for testgen/python)."
    def execute(self):
        _entryPoint()
MVStoreTest.declare(TestBasics1)

if __name__ == '__main__':
    lT = TestBasics1()
    lT.execute()
