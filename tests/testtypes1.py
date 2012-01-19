#!/usr/bin/env python2.6
"""This module verifies that the python library handles properly each data type,
via both pathSQL and protobuf."""

from testfwk import MVStoreTest
from mvstore import *
import datetime

def _entryPoint():
    lMvStore = MVSTORE()
    lMvStore.open()

    # VT_STRING
    lValue = "Hello how are you";
    lPin = PIN.loadPINs(lMvStore.qProto("INSERT (\"http://localhost/mv/property/testtypes1/value1\") VALUES ('%s');" % lValue))[0]
    lPin['http://localhost/mv/property/testtypes1/value2'] = lValue
    lPin.refreshPIN()
    assert lValue == lPin['http://localhost/mv/property/testtypes1/value1']
    assert lValue == lPin['http://localhost/mv/property/testtypes1/value2']
    assert mvstore_pb2.Value.VT_STRING == lPin.getExtra('http://localhost/mv/property/testtypes1/value1').mType
    assert mvstore_pb2.Value.VT_STRING == lPin.getExtra('http://localhost/mv/property/testtypes1/value2').mType

    # VT_BSTR
    lValue = bytearray(b"Hello how are you")
    lPin = PIN.loadPINs(lMvStore.qProto("INSERT (\"http://localhost/mv/property/testtypes1/value1\") VALUES (X'%s');" % ''.join("%02x" % iB for iB in lValue)))[0]
    lPin['http://localhost/mv/property/testtypes1/value2'] = lValue
    lPin.refreshPIN()
    assert lValue == lPin['http://localhost/mv/property/testtypes1/value1']
    assert lValue == lPin['http://localhost/mv/property/testtypes1/value2']
    assert mvstore_pb2.Value.VT_BSTR == lPin.getExtra('http://localhost/mv/property/testtypes1/value1').mType
    assert mvstore_pb2.Value.VT_BSTR == lPin.getExtra('http://localhost/mv/property/testtypes1/value2').mType
    
    # VT_URL
    lValue = "urn:issn:1234-5678"
    lPin = PIN.loadPINs(lMvStore.qProto("INSERT (\"http://localhost/mv/property/testtypes1/value1\") VALUES (U'%s');" % lValue))[0]
    lPin['http://localhost/mv/property/testtypes1/value2'] = PIN.Url(lValue)
    lPin.refreshPIN()
    assert lValue == lPin['http://localhost/mv/property/testtypes1/value1']
    assert lValue == lPin['http://localhost/mv/property/testtypes1/value2']
    assert mvstore_pb2.Value.VT_URL == lPin.getExtra('http://localhost/mv/property/testtypes1/value1').mType
    assert mvstore_pb2.Value.VT_URL == lPin.getExtra('http://localhost/mv/property/testtypes1/value2').mType
    
    # VT_INT
    lValue = 12345
    lPin = PIN.loadPINs(lMvStore.qProto("INSERT (\"http://localhost/mv/property/testtypes1/value1\") VALUES (%d);" % lValue))[0]
    lPin['http://localhost/mv/property/testtypes1/value2'] = lValue
    lPin.refreshPIN()
    assert lValue == lPin['http://localhost/mv/property/testtypes1/value1']
    assert lValue == lPin['http://localhost/mv/property/testtypes1/value2']
    assert mvstore_pb2.Value.VT_INT == lPin.getExtra('http://localhost/mv/property/testtypes1/value1').mType
    assert mvstore_pb2.Value.VT_INT == lPin.getExtra('http://localhost/mv/property/testtypes1/value2').mType

    # VT_UINT
    lValue = 12345
    lPin = PIN.loadPINs(lMvStore.qProto("INSERT (\"http://localhost/mv/property/testtypes1/value1\") VALUES (%du);" % lValue))[0]
    lPin['http://localhost/mv/property/testtypes1/value2'] = (lValue, PIN.Extra(pType=mvstore_pb2.Value.VT_UINT))
    lPin.refreshPIN()
    assert lValue == lPin['http://localhost/mv/property/testtypes1/value1']
    assert lValue == lPin['http://localhost/mv/property/testtypes1/value2']
    assert mvstore_pb2.Value.VT_UINT == lPin.getExtra('http://localhost/mv/property/testtypes1/value1').mType
    assert mvstore_pb2.Value.VT_UINT == lPin.getExtra('http://localhost/mv/property/testtypes1/value2').mType

    # VT_INT64
    lValue = -8589934592
    lPin = PIN.loadPINs(lMvStore.qProto("INSERT (\"http://localhost/mv/property/testtypes1/value1\") VALUES (%d);" % lValue))[0]
    lPin['http://localhost/mv/property/testtypes1/value2'] = lValue
    lPin.refreshPIN()
    assert lValue == lPin['http://localhost/mv/property/testtypes1/value1']
    assert lValue == lPin['http://localhost/mv/property/testtypes1/value2']
    assert mvstore_pb2.Value.VT_INT64 == lPin.getExtra('http://localhost/mv/property/testtypes1/value1').mType
    assert mvstore_pb2.Value.VT_INT64 == lPin.getExtra('http://localhost/mv/property/testtypes1/value2').mType

    # VT_UINT64
    lValue = 8589934592
    lPin = PIN.loadPINs(lMvStore.qProto("INSERT (\"http://localhost/mv/property/testtypes1/value1\") VALUES (%dU);" % lValue))[0]
    lPin['http://localhost/mv/property/testtypes1/value2'] = (lValue, PIN.Extra(pType=mvstore_pb2.Value.VT_UINT64))
    lPin.refreshPIN()
    assert lValue == lPin['http://localhost/mv/property/testtypes1/value1']
    assert lValue == lPin['http://localhost/mv/property/testtypes1/value2']
    assert mvstore_pb2.Value.VT_UINT64 == lPin.getExtra('http://localhost/mv/property/testtypes1/value1').mType
    assert mvstore_pb2.Value.VT_UINT64 == lPin.getExtra('http://localhost/mv/property/testtypes1/value2').mType

    # VT_FLOAT
    lValue = 123.5
    lPin = PIN.loadPINs(lMvStore.qProto("INSERT (\"http://localhost/mv/property/testtypes1/value1\") VALUES (%sf);" % lValue))[0]
    lPin['http://localhost/mv/property/testtypes1/value2'] = (lValue, PIN.Extra(pType=mvstore_pb2.Value.VT_FLOAT))
    lPin.refreshPIN()
    assert lValue == lPin['http://localhost/mv/property/testtypes1/value1']
    assert lValue == lPin['http://localhost/mv/property/testtypes1/value2']
    assert mvstore_pb2.Value.VT_FLOAT == lPin.getExtra('http://localhost/mv/property/testtypes1/value1').mType
    assert mvstore_pb2.Value.VT_FLOAT == lPin.getExtra('http://localhost/mv/property/testtypes1/value2').mType

    # VT_DOUBLE
    lValue = 123.5
    lPin = PIN.loadPINs(lMvStore.qProto("INSERT (\"http://localhost/mv/property/testtypes1/value1\") VALUES (%s);" % lValue))[0]
    lPin['http://localhost/mv/property/testtypes1/value2'] = lValue
    lPin.refreshPIN()
    assert lValue == lPin['http://localhost/mv/property/testtypes1/value1']
    assert lValue == lPin['http://localhost/mv/property/testtypes1/value2']
    assert mvstore_pb2.Value.VT_DOUBLE == lPin.getExtra('http://localhost/mv/property/testtypes1/value1').mType
    assert mvstore_pb2.Value.VT_DOUBLE == lPin.getExtra('http://localhost/mv/property/testtypes1/value2').mType

    # VT_BOOL
    lValue = True
    lPin = PIN.loadPINs(lMvStore.qProto("INSERT (\"http://localhost/mv/property/testtypes1/value1\") VALUES (%s);" % lValue))[0]
    lPin['http://localhost/mv/property/testtypes1/value2'] = lValue
    lPin.refreshPIN()
    assert lValue == lPin['http://localhost/mv/property/testtypes1/value1']
    assert lValue == lPin['http://localhost/mv/property/testtypes1/value2']
    assert mvstore_pb2.Value.VT_BOOL == lPin.getExtra('http://localhost/mv/property/testtypes1/value1').mType
    assert mvstore_pb2.Value.VT_BOOL == lPin.getExtra('http://localhost/mv/property/testtypes1/value2').mType

    # VT_DATETIME
    # Review: timezone handling seems strange...
    lValue = datetime.datetime.utcnow()
    lValue2 = lValue.replace(tzinfo=None) - datetime.timedelta(seconds=time.timezone)
    lValueStr = MVStoreTest.strftime(lValue, "%4Y-%2m-%2d %2H:%2M:%2S.%f")
    lPin = PIN.loadPINs(lMvStore.qProto("INSERT (\"http://localhost/mv/property/testtypes1/value1\") VALUES (TIMESTAMP'%s');" % lValueStr))[0]
    lPin['http://localhost/mv/property/testtypes1/value2'] = lValue2
    lPin.refreshPIN()
    assert lValue2.timetuple() == lPin['http://localhost/mv/property/testtypes1/value1'].timetuple()
    assert lValue2.timetuple() == lPin['http://localhost/mv/property/testtypes1/value2'].timetuple()
    assert mvstore_pb2.Value.VT_DATETIME == lPin.getExtra('http://localhost/mv/property/testtypes1/value1').mType
    assert mvstore_pb2.Value.VT_DATETIME == lPin.getExtra('http://localhost/mv/property/testtypes1/value2').mType
    lMvStore.qProto("UPDATE %s ADD \"http://localhost/mv/property/testtypes1/value1\"=%s;" % (lPin.mPID, 123))
    lPin.refreshPIN()
    lReferenced1 = lPin

    # VT_INTERVAL
    lValue = datetime.timedelta(seconds=123.5)
    lValueStr = MVStoreTest.strftime((datetime.datetime(year=1970, month=1, day=1) + lValue), "%2H:%2M:%2S.%f")
    lPin = PIN.loadPINs(lMvStore.qProto("INSERT (\"http://localhost/mv/property/testtypes1/value1\") VALUES (INTERVAL'%s');" % lValueStr))[0]
    lPin['http://localhost/mv/property/testtypes1/value2'] = lValue
    lPin.refreshPIN()
    assert lValue == lPin['http://localhost/mv/property/testtypes1/value1']
    assert lValue == lPin['http://localhost/mv/property/testtypes1/value2']
    assert mvstore_pb2.Value.VT_INTERVAL == lPin.getExtra('http://localhost/mv/property/testtypes1/value1').mType
    assert mvstore_pb2.Value.VT_INTERVAL == lPin.getExtra('http://localhost/mv/property/testtypes1/value2').mType

    # VT_REFID
    lValue = PIN.Ref.fromPID(lReferenced1.mPID)
    lPin = PIN.loadPINs(lMvStore.qProto("INSERT (\"http://localhost/mv/property/testtypes1/value1\") VALUES (%s);" % lReferenced1.mPID))[0]
    lPin['http://localhost/mv/property/testtypes1/value2'] = lValue
    lPin.refreshPIN()
    assert lValue == lPin['http://localhost/mv/property/testtypes1/value1']
    assert lValue == lPin['http://localhost/mv/property/testtypes1/value2']
    assert mvstore_pb2.Value.VT_REFID == lPin.getExtra('http://localhost/mv/property/testtypes1/value1').mType
    assert mvstore_pb2.Value.VT_REFID == lPin.getExtra('http://localhost/mv/property/testtypes1/value2').mType

    # VT_REFIDPROP
    lValue = PIN.Ref(pLocalPID=lReferenced1.mPID.mLocalPID, pIdent=lReferenced1.mPID.mIdent, pProperty='http://localhost/mv/property/testtypes1/value1')
    lPin = PIN.loadPINs(lMvStore.qProto("INSERT (\"http://localhost/mv/property/testtypes1/value1\") VALUES (%s.\"http://localhost/mv/property/testtypes1/value1\");" % lReferenced1.mPID))[0]
    lPin['http://localhost/mv/property/testtypes1/value2'] = lValue
    lPin.refreshPIN()
    assert lValue == lPin['http://localhost/mv/property/testtypes1/value1']
    assert lValue == lPin['http://localhost/mv/property/testtypes1/value2']
    assert mvstore_pb2.Value.VT_REFIDPROP == lPin.getExtra('http://localhost/mv/property/testtypes1/value1').mType
    assert mvstore_pb2.Value.VT_REFIDPROP == lPin.getExtra('http://localhost/mv/property/testtypes1/value2').mType
    
    # VT_REFIDELT    
    lValue = PIN.Ref(pLocalPID=lReferenced1.mPID.mLocalPID, pIdent=lReferenced1.mPID.mIdent, pProperty='http://localhost/mv/property/testtypes1/value1', pEid=lReferenced1.getExtra('http://localhost/mv/property/testtypes1/value1', pEpos=1).mEid)
    lPin = PIN.loadPINs(lMvStore.qProto("INSERT (\"http://localhost/mv/property/testtypes1/value1\") VALUES (%s.\"http://localhost/mv/property/testtypes1/value1\"[%d]);" % (lReferenced1.mPID, lValue.mEid)))[0]
    lPin['http://localhost/mv/property/testtypes1/value2'] = lValue
    lPin.refreshPIN()
    assert lValue == lPin['http://localhost/mv/property/testtypes1/value1']
    assert lValue == lPin['http://localhost/mv/property/testtypes1/value2']
    assert mvstore_pb2.Value.VT_REFIDELT == lPin.getExtra('http://localhost/mv/property/testtypes1/value1').mType
    assert mvstore_pb2.Value.VT_REFIDELT == lPin.getExtra('http://localhost/mv/property/testtypes1/value2').mType    
    
    # TODO: VT_EXPR
    # TODO: VT_QUERY
    # TODO: VT_CURRENT - available?
    # TODO: VT_ENUM - available?
    # TODO: VT_DECIMAL - available?
    # TODO: VT_URIID - purpose? vs VT_REFID?
    # TODO: VT_IDENTITY - purpose?
    # TODO: VT_REFCID - purpose?
    # TODO: VT_RANGE - purpose?
    # NOTE: VT_ARRAY is tested with collections.

    lMvStore.close()

class TestTypes1(MVStoreTest):
    "A basic test for native data types (python and mvstore)."
    def execute(self):
        _entryPoint()
MVStoreTest.declare(TestTypes1)

if __name__ == '__main__':
    lT = TestTypes1()
    lT.execute()
