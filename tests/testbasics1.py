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
from testfwk import AffinityTest
from affinity import *
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
    lAffinity = AFFINITY()
    lAffinity.open()

    print ("\nA basic transaction, with pin creations (including collections).")
    lAffinity.startTx()
    PIN.savePINs([
        PIN({"http://localhost/afy/property/testbasics1/myprop1":"salut", "http://localhost/afy/property/testbasics1/myprop2":123}), 
        PIN({"http://localhost/afy/property/testbasics1/myprop1":"bonjour", "http://localhost/afy/property/testbasics1/myprop3":456})])
    lPinDef = {"http://localhost/afy/property/testbasics1/myprop1":["comment", "allez", "vous", "donc"], "http://localhost/afy/property/testbasics1/myprop2":123.5, "http://localhost/afy/property/testbasics1/myprop3":True, "http://localhost/afy/property/testbasics1/myprop4":"-65"}
    lPin = PIN(lPinDef).savePIN()
    lAffinity.commitTx()

    print ("\nBasic examples of pathSQL commands.")
    lAffinity.q("insert (\"http://localhost/afy/property/testbasics1/potato_color\") values (10);") # Note: sent directly to the server, as a string.
    lAffinity.qProto("insert (\"http://localhost/afy/property/testbasics1/potato_color\") values (11);") # Note: sent to the server as a query embedded inside a protobuf.
    lPins = PIN.loadPINs(lAffinity.qProto("SELECT * WHERE EXISTS(\"http://localhost/afy/property/testbasics1/myprop1\");"))
    print ("result of SELECT * WHERE EXISTS(\"http://localhost/afy/property/testbasics1/myprop1\"): %s" % lPins)
    try:
        lAffinity.qProto("CREATE CLASS \"http://localhost/afy/class/testbasics1/c1\" AS SELECT * WHERE EXISTS(\"http://localhost/afy/property/testbasics1/myprop1\");") # TODO: check if this really worked (when embedded in protobuf like this)...
    except:
        pass

    print ("\nGet resulting pins.")
    lRaw = lAffinity.q("SELECT * FROM \"http://localhost/afy/class/testbasics1/c1\";")
    lPBStream = affinity_pb2.AfyStream()
    lPBStream.ParseFromString(lRaw)
    if False:
        displayPBStr(lRaw, pTitle="raw result of SELECT * FROM \"http://localhost/afy/class/testbasics1/c1\"")
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
    print ("num values in fetched pin's myprop1 collection: %d" % len(lPin["http://localhost/afy/property/testbasics1/myprop1"]))
    lPin.moveXafterY("http://localhost/afy/property/testbasics1/myprop1", 2, 0)
    print (lPin)
    lPinAfter = PIN.createFromPID(lPin.mPID)
    comparePins(lPin, lPinAfter)
    lPin.refreshPIN()
    
    print ("\nBasic demonstration of in-place modifications (adding myprop5 and modifying myprop2).")
    lPin["http://localhost/afy/property/testbasics1/myprop5"] = "some newly added value"
    lPin["http://localhost/afy/property/testbasics1/myprop2"] = lPin["http://localhost/afy/property/testbasics1/myprop2"] + 50.4321
    del lPin["http://localhost/afy/property/testbasics1/myprop1"][3]
    print (lPin)
    lPinAfter = PIN.createFromPID(lPin.mPID)
    comparePins(lPin, lPinAfter)
    lPin.refreshPIN()

    print ("\nBasic demonstration of OP_PLUS (on myprop4 of pin %s)." % repr(lPin.mPID))
    lValBef = int(lPin["http://localhost/afy/property/testbasics1/myprop4"])
    PIN({"http://localhost/afy/property/testbasics1/myprop4":(-35, PIN.Extra(pType=affinity_pb2.Value.VT_INT, pOp=affinity_pb2.Value.OP_PLUS))}, __PID__=lPin.mPID).savePIN()
    lPin.refreshPIN()
    if int(lPin["http://localhost/afy/property/testbasics1/myprop4"]) != lValBef - 35:
        print ("Unexpected value after OP_PLUS: %d (expected %d)" % (int(lPin["http://localhost/afy/property/testbasics1/myprop4"]), lValBef - 35))
        assert False

    if False:
        print ("\nA simple test for batch-insert of inter-connected uncommitted pins.")
        PIN.savePINs([
            PIN({PIN.SK_PID:1, "http://localhost/afy/property/testbasics1/referenced_by":PIN.Ref(2), "http://localhost/afy/property/testbasics1/text":"1 <- 2"}), 
            PIN({PIN.SK_PID:2, "http://localhost/afy/property/testbasics1/references":PIN.Ref(1), "http://localhost/afy/property/testbasics1/text":"2 -> 1"})])

    if len(lPBStream.pins) > 1:
        print ("\nA basic PIN modification test (ISession::modify style).")
        lPid = lPBStream.pins[1].id
        print ("before modify: %s" % lAffinity.check("SELECT * FROM {@%x};" % lPid.id))
        lPin = PIN({PIN.SK_PID:PIN.PID.fromPB(lPid), "http://localhost/afy/property/testbasics1/myprop1":"bien"})
        #lPin = PIN(__PID__=PIN.PID.fromPB(lPid), testbasics1_myprop1="bien")
        lPin.savePIN()
        print ("after modify: %s" % lAffinity.check("SELECT * FROM {@%x};" % lPid.id))

    if len(lPBStream.pins) > 1:
        print ("\nA basic PIN deletion test.")
        lPid = lPBStream.pins[1].id
        print ("before delete: %s" % lAffinity.check("SELECT * FROM {@%x};" % lPid.id))
        PIN.deletePINs([PIN.PID.fromPB(lPid)])
        #print ("after delete: %s" % lAffinity.check("SELECT * FROM {@%x};" % lPid.id))

    #print ("\nFinal state check.")
    #print (lAffinity.check("SELECT *"))
    lAffinity.close()

class TestBasics1(AffinityTest):
    "First basic test (originally written for testgen/python)."
    def execute(self):
        _entryPoint()
AffinityTest.declare(TestBasics1)

if __name__ == '__main__':
    lT = TestBasics1()
    lT.execute()
