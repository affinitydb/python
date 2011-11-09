#!/usr/bin/env python2.6
"""This module defines the key components of mvStore's low-level client library in python:
MVStoreConnection and PIN (including PIN.PID and PIN.Collection).
The library talks to the store via mvSQL and protobuf exclusively.
When the mvstoreinproc module is in the path, and MVStoreConnection.DEFAULT_INPROC is True,
the library talks to an in-proc store. Otherwise, it uses HTTP to reach the mvserver.
Please read the documentation of each component for more details."""

from __future__ import with_statement
from collections import MutableSequence
from copy import copy
import base64
import datetime
try:
    import httplib # python2
except:    
    import http as httplib # python3
import logging
import mvstore_pb2
try:
    import mvstoreinproc # Optional (for inproc execution of mvstore; see the 'ext' subdirectory).
except Exception as ex:
    pass
import os
import string
import subprocess
import sys
import threading
import time
import traceback
import urllib2
import urllib

# For MVHTTPResponse.
try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO

# Workaround (these enum values can't be provided by mvstore.proto).
EID_COLLECTION = 4294967295
EID_LAST_ELEMENT = 4294967294
EID_FIRST_ELEMENT = 4294967293

# Names for special properties.
SP_PROPERTY_NAMES = \
{ \
    mvstore_pb2.SP_PINID:"mv:pinID", \
    mvstore_pb2.SP_DOCUMENT:"mv:document", \
    mvstore_pb2.SP_PARENT:"mv:parent", \
    mvstore_pb2.SP_VALUE:"mv:value", \
    mvstore_pb2.SP_CREATED:"mv:created", \
    mvstore_pb2.SP_CREATEDBY:"mv:createdBy", \
    mvstore_pb2.SP_UPDATED:"mv:updated", \
    mvstore_pb2.SP_UPDATEDBY:"mv:updatedBy", \
    mvstore_pb2.SP_ACL:"mv:ACL", \
    mvstore_pb2.SP_URI:"mv:URI", \
    mvstore_pb2.SP_STAMP:"mv:stamp", \
    mvstore_pb2.SP_CLASSID:"mv:classID", \
    mvstore_pb2.SP_PREDICATE:"mv:predicate", \
    mvstore_pb2.SP_NINSTANCES:"mv:nInstances", \
    mvstore_pb2.SP_NDINSTANCES:"mv:nDelInstances", \
    mvstore_pb2.SP_SUBCLASSES:"mv:subclasses", \
    mvstore_pb2.SP_SUPERCLASSES:"mv:superclasses", \
    mvstore_pb2.SP_CLASS_INFO:"mv:classInfo", \
    mvstore_pb2.SP_INDEX_INFO:"mv:indexInfo", \
    mvstore_pb2.SP_PROPERTIES:"mv:properties", \
}    

# Internal logging.
def configureMvLogging():
    "Configure the logging behavior of this module."
    lFormat = '%(asctime)s|%(levelname)s|%(filename)s|%(funcName)s(): %(message)s [tid=%(thread)d]'
    logging.basicConfig(filename='mvstore_py.log', level=logging.WARN, format=lFormat)
    lConsole = logging.StreamHandler()
    lConsole.setLevel(logging.WARN)
    lConsole.setFormatter(logging.Formatter(lFormat))
    logging.getLogger().addHandler(lConsole)
configureMvLogging() # Review: May remove this by default (arguably belongs to the app).

# Misc. Helpers.
def displayPBStr(pPBStr, pTitle=None):
    "[internal] Invoke the command-line 'protoc --decode' to produce a readable form of the pPBStr serialized protobuf string."
    lP = subprocess.Popen(["protoc", "--decode=MVStorePB.MVStream", "--proto_path=../kernel/src/", "../kernel/src/mvstore.proto"], shell=False, stdout=subprocess.PIPE, stdin=subprocess.PIPE)
    #lP = subprocess.Popen(["protoc", "--decode_raw"], shell=False, stdout=subprocess.PIPE, stdin=subprocess.PIPE)
    lOut = lP.communicate(input=pPBStr)[0]
    #lOut = filter(lambda c: c==" " or c not in string.whitespace, lOut) # Keep spaces but not crlf, tabs etc.
    if pTitle:
        print ("%s: %s" % (pTitle, lOut))
    else:
        print (lOut)
def savePBStr(pPBStr, pOutputFileName):
    "[internal] Save the pPBStr serialized protobuf string to a binary file pOutputFileName."
    lF = open(pOutputFileName, "wb")
    lF.write(pPBStr)
    lF.close()
def parsePBStr(pPBStr):
    "[internal] Parse the raw pPBStr and returned the resulting protobuf structure."
    lPBStream = mvstore_pb2.MVStream()
    try:
        lPBStream.ParseFromString(pPBStr)
        return lPBStream
    except Exception as ex:
        print ("***")
        print ("*** EXCEPTION CAUGHT in ParseFromString:\n***   %s" % ex)
        print ("***   waiting for 5 seconds..."); time.sleep(5); savePBStr(pPBStr, "/tmp/mvstore_pb_issue.raw")
        print ("***")
        return None
def isInteger(p):
    "Return True if p is an integer value."
    return hasattr(p, '__mod__') and 0 == (p % 1)

# Exceptions.
class InvalidParameter(Exception):
    "Invalid or unexpected parameter."

# MVHTTPResponse: HTTPResponse override.
# Note: httplib.HTTPResponse is an old-style class...
class MVHTTPResponse(httplib.HTTPResponse):
    """HTTPResponse override, to enable streaming (i.e. reading response segments, corresponding to
    flushed segments in the input stream)."""
    def begin(self):
        lR = httplib.HTTPResponse.begin(self)
        self.will_close = 0
        return lR
    def read(self, amt=None):
        if self.fp is None or self.chunked:
            return httplib.HTTPResponse.read(self, amt)
        if amt is None and self.length is None:
            # Note (maxw): From socket.py, _fileobject.read()...
            # Note (maxw): The main difference is that amt==None is not interpreted to mean 'read all', just 'read some'...
            rbufsize = max(self.fp._rbufsize, self.fp.default_bufsize)
            buf = self.fp._rbuf
            buf.seek(0, 2)
            if amt < 0:
                self.fp._rbuf = StringIO()
                data = self.fp._sock.recv(rbufsize)
                if data:
                    buf.write(data)
                return buf.getvalue()
        return httplib.HTTPResponse.read(self, amt)

# Core Objects: store access/connection.
class MVStoreConnection(object):
    """Access layer for mvStore (aka db connection). For convenience, it allows
    to talk to an inproc store or to the server, interchangeably. The global DEFAULT_CONNECTION instance
    is used by default; otherwise, a 'with' statement can be used to push a different connection
    on the stack. Not designed to be used concurrently (use one connection per thread)."""
    FLAG_COUNT_ONLY = 1
    DEFAULT_INPROC = True
    DEFAULT_CONNECTION = None
    @staticmethod
    def isInproc():
        return sys.modules.has_key('mvstoreinproc') and MVStoreConnection.DEFAULT_INPROC
    # ---
    TLS = threading.local()
    @staticmethod
    def getCurrentDbConnection():
        if not MVStoreConnection.TLS.__dict__.has_key("mDbConnectionStack"):
            logging.debug("initializing tls")
            MVStoreConnection.TLS.mDbConnectionStack = [MVStoreConnection.DEFAULT_CONNECTION]
        return MVStoreConnection.TLS.mDbConnectionStack[-1]
    @staticmethod
    def pushDbConnection(pDbConnection):
        if not MVStoreConnection.TLS.__dict__.has_key("mDbConnectionStack"):
            logging.debug("initializing tls")
            MVStoreConnection.TLS.mDbConnectionStack = [MVStoreConnection.DEFAULT_CONNECTION]
        lOld = MVStoreConnection.TLS.mDbConnectionStack[-1]
        logging.debug("pushing db connection %s (old=%s)" % (pDbConnection, lOld))
        MVStoreConnection.TLS.mDbConnectionStack.append(pDbConnection)
        return lOld
    @staticmethod
    def popDbConnection():
        if not MVStoreConnection.TLS.__dict__.has_key("mDbConnectionStack"):
            logging.warn("no connection to pop!")
            return
        logging.debug("popping db connection %s (new=%s)" % (MVStoreConnection.TLS.mDbConnectionStack[-1], MVStoreConnection.TLS.mDbConnectionStack[-2]))
        return MVStoreConnection.TLS.mDbConnectionStack.pop()
    def __enter__(self):
        MVStoreConnection.pushDbConnection(self)
    def __exit__(self, etyp, einst, etb):
        if MVStoreConnection.popDbConnection() != self:
            logging.warn("imbalance detected in the db connection stack!")
    # ---
    class Inproc(object):
        """[internal] Internal implementation, for mvStore running in-process (in python)."""
        def open(self, pKeepAlive): return 0 == mvstoreinproc.open()
        def close(self): return 0 == mvstoreinproc.close()
        def startSession(self): return mvstoreinproc.startSession()
        def terminateSession(self, pSession): return mvstoreinproc.terminateSession(pSession)
        def attachSession(self, pSession): return mvstoreinproc.attachSession(pSession)
        def detachSession(self, pSession): return mvstoreinproc.detachSession(pSession)
        def post(self, pMsg, pSession): return mvstoreinproc.post(pSession, pMsg)
        def beginlongpost(self, pSession): return mvstoreinproc.beginlongpost(pSession)
        def continuelongpost(self, pLPToken, pMsg, pExpectOutput, pSession): logging.debug(pMsg); return mvstoreinproc.continuelongpost(pSession, pLPToken, pMsg)
        def endlongpost(self, pLPToken, pSession): return mvstoreinproc.endlongpost(pSession, pLPToken)
        def get(self, pMsg, pFlags, pSession): logging.debug(pMsg); return mvstoreinproc.get(pSession, pMsg, pFlags)
        def check(self, pMsg, pSession): logging.debug(pMsg); return mvstoreinproc.check(pSession, pMsg)
        def trueSessions(self): return True
    # ---
    class MvServer(object):
        """[internal] Internal implementation, for mvStore running as a server (reached via http)."""
        def __init__(self, pConnection):
            self.mConnection = pConnection
            self.mConnectionHTTP = None
        def open(self, pKeepAlive):
            if pKeepAlive:
                self.mConnectionHTTP = httplib.HTTPConnection(self.host())
            return True
        def close(self):
            if self.mConnectionHTTP:
                self.mConnectionHTTP.close()
                self.mConnectionHTTP = None
            return True
        def startSession(self): return True
        def terminateSession(self, pSession): return True
        def attachSession(self, pSession): return True
        def detachSession(self, pSession): return True
        def post(self, pMsg, pSession):
            logging.debug("sent %d bytes" % len(pMsg))
            lRes = (0, None)
            if self.mConnectionHTTP:
                self.mConnectionHTTP.request("POST", "/db/?i=proto&o=proto", body=pMsg, headers={"Authorization":"Basic %s" % self.auth(), "Content-Type":"application/octet-stream"})
                lRes = (0, self.mConnectionHTTP.getresponse().read())
            else:
                lRes = (0, urllib2.urlopen(urllib2.Request("http://%s/db/?i=proto&o=proto" % self.host(), data=pMsg, headers={"Authorization":"Basic %s" % self.auth(), "Content-Type":"application/octet-stream"})).read()) # print binascii.hexlify(pMsg);
            logging.debug("received %d bytes" % len(lRes[1]))
            return lRes
        def beginlongpost(self, pSession):
            # Note: Currently, a mvstore transaction cannot live across more than one http request...
            lLongC = httplib.HTTPConnection(self.host())
            lLongC.response_class = MVHTTPResponse
            lLongC.putrequest('POST', '/db/?i=proto&o=proto')
            lLongC.putheader("Authorization", "Basic %s" % self.auth())
            lLongC.putheader("Content-Type", "application/octet-stream")
            lLongC.endheaders()
            self.mResponse = None
            logging.debug("token=%s" % lLongC)
            return lLongC
        def continuelongpost(self, pLPToken, pMsg, pExpectOutput, pSession):
            logging.debug("sent %d bytes, token=%s" % (len(pMsg), pLPToken))
            pLPToken.send(pMsg)
            if self.mResponse == None:
                self.mResponse = pLPToken.getresponse()
            if pExpectOutput:
                lRes = (0, self.mResponse.read())
                logging.debug("received %d bytes, token=%s" % (len(lRes[1]), pLPToken))
            else:
                lRes = (0, None)
                logging.debug("expected&received 0 byte, token=%s" % pLPToken)
            return lRes
        def endlongpost(self, pLPToken, pSession):
            logging.debug("token=%s" % pLPToken)
            pLPToken.close()
            self.mResponse = None
        def get(self, pMsg, pFlags, pSession):
            logging.debug(pMsg)
            lRet = None
            if self.mConnectionHTTP:
                self.mConnectionHTTP.request("GET", "/db/?q=%s&i=mvsql%s" % (urllib.quote(pMsg), ("&o=proto", "&type=count")[pFlags==MVStoreConnection.FLAG_COUNT_ONLY]), headers={"Authorization":"Basic %s" % self.auth()})
                lRet = self.mConnectionHTTP.getresponse().read()
            else:
                lRet = urllib2.urlopen(urllib2.Request("http://%s/db/?q=%s&i=mvsql%s" % (self.host(), urllib.quote(pMsg), ("&o=proto", "&type=count")[pFlags==MVStoreConnection.FLAG_COUNT_ONLY]), headers={"Authorization":"Basic %s" % self.auth()})).read()
            if pFlags==MVStoreConnection.FLAG_COUNT_ONLY:
                return int(lRet)
            return lRet
        def check(self, pMsg, pSession):
            logging.debug(pMsg)
            return urllib2.urlopen(urllib2.Request("http://%s/db/?q=%s&i=mvsql&o=json" % (self.host(), urllib.quote(pMsg)), headers={"Authorization":"Basic %s" % self.auth()})).read()
        def trueSessions(self): return False
        def host(self): return self.mConnection.host()
        def auth(self): return self.mConnection.basicauth()
    # ---
    class PBTransactionCtx(object):
        """[internal] Transaction context for protobuf. Allows to run long transactions and fetch intermediate results
        (without any dependency on a keep-alive connection). Facilitates the concatenation of protobuf logical MVStream segments,
        to produce the final outgoing stream."""
        MODE_IGNORE_OUTPUT = 0x0001
        MODE_IMMEDIATE_UPDATES = 0x0002
        NEXT_CID = 1 # Review: thread-safety
        # ---
        def __init__(self, pConnection=None, pMode=0):
            self.mConnection = pConnection
            if not self.mConnection:
                self.mConnection = MVStoreConnection.getCurrentDbConnection()
            self.mPBStream = mvstore_pb2.MVStream() # The current stream segment.
            self.mSegments = [] # The accumulated stream segments.
            self.mSegmentsExpectOutput = False # Maybe just a workaround until commit produces bits in the response stream...
            self.mMode = pMode # The combination of modes in which we operate currently.
            self.mRC = None # The return code from mvStore.
            self.mPBOutput = None # The parsed (MVStream) protobuf output.
            self.mPropDict = {} # Accumulated dictionary of {propname, propid}.
            self.mLPToken = None # For long-running transactions (protobuf).
            self.mTxCnt = 0 # Holds a count of nested transactions.
            self.mPINUpdates = [] # Accumulates PIN updates during a transaction.
        # ---
        # Control of the PB stream.
        def isOutputIgnored(self):
            return ((self.mMode & MVStoreConnection.PBTransactionCtx.MODE_IGNORE_OUTPUT) != 0)
        def performImmediateUpdates(self):
            return ((self.mMode & MVStoreConnection.PBTransactionCtx.MODE_IMMEDIATE_UPDATES) != 0)
        def capture(self):
            if len(self.mPBStream.pins) > 0:
                # Review: The final condition will be different, but for the moment I'm not sure I can do better.
                for iP in self.mPBStream.pins:
                    if mvstore_pb2.MVStream.OP_INSERT == iP.op:
                        self.mSegmentsExpectOutput = True
                        break
                self.mSegments.append(self.mPBStream.SerializeToString())
            elif len(self.mPBStream.stmt) > 0:
                self.mSegmentsExpectOutput = True
                self.mSegments.append(self.mPBStream.SerializeToString())
            elif len(self.mPBStream.txop) > 0 or len(self.mPBStream.flush) > 0 or len(self.mPBStream.properties) > 0:
                self.mSegments.append(self.mPBStream.SerializeToString())
            else:
                logging.warn("An empty mPBStream was captured... and ignored.")
            logging.debug("%s segments, %s bytes%s" % (len(self.mSegments), sum([len(iS) for iS in self.mSegments]), ("", ", FLUSH")[len(self.mPBStream.flush) > 0]))
            self.mPBStream = mvstore_pb2.MVStream()
        def flush(self, pExplicit=True):
            if pExplicit:
                self.mPBStream.flush.append(0)
            self.capture()
            self._applyPINUpdates()
            self._pushData()            
        def getPBStream(self):
            return self.mPBStream
        # ---
        # Transaction control.
        # TODO: Offer the commit/rollback ALL option.
        def startTx(self):
            logging.debug("")
            if not self.mLPToken:
                self.mLPToken = self.mConnection._beginlongpost()
            self.mPBStream.txop.append(mvstore_pb2.MVStream.TX_START)
            self.capture()
            self.mTxCnt += 1
        def commitTx(self):
            logging.debug("")
            self.mPBStream.txop.append(mvstore_pb2.MVStream.TX_COMMIT)
            self.capture()
            self.mTxCnt -= 1
            if 0 == self.mTxCnt:
                self._terminate()
        def rollbackTx(self):
            logging.debug("")
            self.mPBStream.txop.append(mvstore_pb2.MVStream.TX_ROLLBACK)
            self.capture()
            self.mTxCnt -= 1
            if 0 == self.mTxCnt:
                self._terminate()
        def _terminate(self):
            if self.mTxCnt > 0:
                logging.warn("terminated a txctx prematurely")
            self._applyPINUpdates()
            self._pushData()
            if self.mLPToken:
                self.mConnection._endlongpost(self.mLPToken)
                self.mLPToken = None
            self.mPropDict = {} # REVIEW: In a near future we'll try to be more efficient than this.
            self.mConnection.mTxCtx = None
        # ---
        # Query via protobuf (various flavors).
        def _queryPB1(self, pQstr, pRtt=mvstore_pb2.RT_PINS):
            "This version really participates to the current protobuf stream and its current transaction (i.e. protobuf in&out)."
            lStmt = self.getPBStream().stmt.add()
            lStmt.sq = pQstr
            lStmt.cid = MVStoreConnection.PBTransactionCtx.NEXT_CID; MVStoreConnection.PBTransactionCtx.NEXT_CID += 1
            lStmt.rtt = pRtt
            lStmt.limit = 99999 # otherwise 0 by default right now
            lStmt.offset = 0
            self.flush()
            return self.mPBOutput
        @staticmethod
        def _queryPBOut(pQstr):
            "This version borrows the current connection to request directly from the server a protobuf response (i.e. mvsql in & protobuf out; for debugging etc.)."
            lRaw = MVStoreConnection.getCurrentDbConnection().mvsql(pQstr)
            if lRaw == None:
                return None
            #displayPBStr(lRaw, pTitle="response obtained from mvstore for _queryPB")
            return parsePBStr(lRaw)
        def _queryPB2(self, pQstr):
            return MVStoreConnection.PBTransactionCtx._queryPBOut(pQstr)
        def queryPB(self, pQstr):
            return self._queryPB2(pQstr) # TODO: switch to _queryPB1 when it's glitchless... (right now it's still buggier somehow).
        # ---
        # Accumulation of PIN updates.
        def recordPINUpdate(self, pPINUpdate): # TODO: also record the original pin, to pad its ids
            "Record a PIN update (allows to defer dialogue with mvStore in some cases, and reduce chattiness)."
            if self.performImmediateUpdates():
                raise Exception
            logging.debug("")
            self.mPINUpdates.append(pPINUpdate)
        def _applyPINUpdates(self):
            "Apply all accumulated PIN updates."
            logging.debug("%s updates" % len(self.mPINUpdates))
            if len(self.mPINUpdates) > 0:
                PIN._savePINsi(self.mPINUpdates, self)
                self.mPINUpdates = []
        # ---
        # Accumulation of protobuf segments.
        def _pushData(self):
            "Push all accumulated serialized protobuf segments to mvStore; parse and store the output."
            lSegmentsExpectOutput = self.mSegmentsExpectOutput
            logging.debug("%s segments" % len(self.mSegments))
            lMessage = "".join(self.mSegments)
            if 0 == self.mTxCnt:
                self.mPropDict = {} # REVIEW: In a near future we'll try to be more efficient than this.
            self.mSegments = []
            self.mSegmentsExpectOutput = False
            self.mRC = None
            if 0 == len(lMessage):
                logging.debug("no message to send")
                return
            #displayPBStr(lMessage, pTitle="message sent to mvstore")
            #savePBStr(lMessage, "./sent01.pbdata")
            if self.mLPToken:
                self.mRC, lRawOutput = self.mConnection._continuelongpost(self.mLPToken, lMessage, lSegmentsExpectOutput)
                #displayPBStr(lRawOutput, pTitle="response obtained from mvstore (longpost)")
                if lSegmentsExpectOutput and lRawOutput:
                    logging.debug("result: RC=%s (%s bytes)" % (self.mRC, len(lRawOutput)))
                    self.mPBOutput = parsePBStr(lRawOutput)
                else:
                    logging.debug("result: RC=%s" % self.mRC)
            else:
                self.mRC, lRawOutput = self.mConnection._post(lMessage)
                #displayPBStr(lRawOutput, pTitle="response obtained from mvstore")
                if lSegmentsExpectOutput and lRawOutput:
                    logging.debug("result: RC=%s (%s bytes)" % (self.mRC, len(lRawOutput)))
                    self.mPBOutput = parsePBStr(lRawOutput)
                else:
                    logging.debug("result: RC=%s" % self.mRC)
    # ---
    def __init__(self, pHost="localhost", pPort=4560, pOwner="generic", pPassword=None):
        self.mHost = pHost
        self.mPort = pPort
        self.mOwner = pOwner
        self.mPassword = pPassword
        if MVStoreConnection.isInproc():
            self.mImpl = self.Inproc()
        else:
            self.mImpl = self.MvServer(self)
        self.mSessionStack = []
        self.mTxCtx = None
    def open(self, pKeepAlive=False):
        if self.mImpl.open(pKeepAlive):
            self.startSession()
            return True
        return False
    def close(self):
        self.terminateSession()
        return self.mImpl.close()
    def host(self): return "%s:%s" % (self.mHost, self.mPort)
    def basicauth(self): return base64.b64encode("%s:%s" % (self.mOwner, ("", self.mPassword)[None != self.mPassword]))
    def mvsql(self, pMsg, pFlags=0, pSession=None): return self.mImpl.get(pMsg, pFlags, self._s(pSession))
    def mvsqlProto(self, pQstr): return self._txCtx().queryPB(pQstr)
    def check(self, pMsg, pSession=None): return self.mImpl.check(pMsg, self._s(pSession))
    def startTx(self): self._txCtx().startTx()
    def commitTx(self): self._txCtx().commitTx()
    def rollbackTx(self): self._txCtx().rollbackTx()
    # --- WARNING: This section may become deprecated...
    def trueSessions(self): return self.mImpl.trueSessions()
    def startSession(self): self.mSessionStack.append(self.mImpl.startSession()); return self.mSessionStack[-1]
    def terminateSession(self, pSession=None): return self.mImpl.terminateSession(self._s(pSession))
    def attachSession(self, pSession=None):
        lRet = 0
        lSession = self._s(pSession)
        if len(self.mSessionStack) == 0 or self.mSessionStack[-1] != lSession:
            lRet = self.mImpl.attachSession(lSession)
        self.mSessionStack.append(lSession)
        return lRet
    def detachSession(self, pSession=None):
        lRet = 0
        lSession = self._s(pSession)
        if self.mSessionStack.pop() != lSession:
            logging.warn("pSession didn't match the stack!")
        if len(self.mSessionStack) == 0 or self.mSessionStack[-1] != lSession:
            lRet = self.mImpl.detachSession(lSession)
            if len(self.mSessionStack) > 0:
                self.mImpl.attachSession(self.mSessionStack[-1])
        return lRet
    # ---
    def _post(self, pMsg, pSession=None): return self.mImpl.post(pMsg, self._s(pSession))
    def _beginlongpost(self, pSession=None): return self.mImpl.beginlongpost(self._s(pSession))
    def _continuelongpost(self, pLPToken, pMsg, pExpectOutput, pSession=None): return self.mImpl.continuelongpost(pLPToken, pMsg, pExpectOutput, self._s(pSession))
    def _endlongpost(self, pLPToken, pSession=None): self.mImpl.endlongpost(pLPToken, self._s(pSession))
    def _s(self, pSession): return (pSession, len(self.mSessionStack) > 0 and self.mSessionStack[-1])[pSession == None]
    def _txCtx(self):
        if None == self.mTxCtx:
            self.mTxCtx = MVStoreConnection.PBTransactionCtx(pConnection=self)
        return self.mTxCtx
MVStoreConnection.DEFAULT_CONNECTION = MVStoreConnection()
def MVSTORE(): return MVStoreConnection.getCurrentDbConnection()
    
# Core Objects: response stream reading context.
# Note: In a majority of cases the developer needs not be aware of this (PIN.loadPINs hides it).
class PBReadCtx(object):
    """In-memory representation of the global contextual information returned by mvStore in a response stream."""
    def __init__(self, pPBStream):
        self.mPBStream = pPBStream
        self.mPropID2Name = {}
        self.mPropName2ID = {}
        self.mIdentMap = {}
        if (pPBStream.HasField('owner')):
            self.mOwner = pPBStream.owner
        if (pPBStream.HasField('storeID')):
            self.mStoreID = pPBStream.storeID
        lProps = pPBStream.properties
        for iP in pPBStream.properties:
            self.mPropID2Name[iP.id] = iP.str
            self.mPropName2ID[iP.str] = iP.id
        for iP in SP_PROPERTY_NAMES.items():
            self.mPropID2Name[iP[0]] = iP[1]
            self.mPropName2ID[iP[1]] = iP[0]
        lIdents = pPBStream.identities
        for iI in pPBStream.identities:
            self.mIdentMap[iI.id] = iI.str
    def getPBStream(self): return self.mPBStream
    def getPropName(self, pPropID): return self.mPropID2Name.get(pPropID)
    def getPropID(self, pPropName): return self.mPropName2ID.get(pPropName)
    def getIdentityName(self, pIdentityID): return self.mIdentMap.get(pIdentityID)
    def getOwner(self): return self.mOwner
    def getStoreID(self): return self.mStoreID

# Core Objects: PIN.
class PIN(dict):
    """Convenient in-memory representation of the PIN, or of modifications to a PIN, primarily as a python dictionary of {property name, scalar value or list of values}.
    Also holds another dictionary in the background (mExtras), to contain all the per-value info that is required for specific operations.
    Preserves the standard dictionary interface, and allows to substitute any value with a (value, PIN.Extra) tuple.
    The 'extras' are never visible through normal access, but can be obtained (or modified) upon explicit request.
    The PIN is implemented as a dictionary with keys and values, instead of as a python object with properties.
    One justification is simplicity: even if we supported properties, we'd still need to support dictionaries
    as well. Another reason is to convey the fact that this is a low-level access library, not an object-oriented database
    (or object mapping) layer: the client is invited to _use_ PIN objects to control its DB access, not to
    _conform_ with a class hierarchy or metaclasses or interfaces (we don't interfere with the client's
    application design)."""
    # Note: For simplicity, mExtras always contains lists, even for scalar values.
    # TODO: If needed, may offer a function to return the full representation with all extras.
    #--------
    # PUBLIC: Constants.
    #--------
    # Special Keys.
    SK_PID = "__PID__"
    SK_UPDATE = "__UPD__"
    # mvstore time offset (1600 vs 1970).
    TIME_OFFSET = (datetime.datetime(1970,1,1) - datetime.datetime(1601,1,1)).days * 24 * 60 * 60 * 1000000
    #--------
    # PUBLIC: Collection.
    #--------
    class Collection(MutableSequence):
        """In-memory representation of collections, overriding the natural 'list' representation to track
        changes through native/std python methods for lists, and make them persistent. Not self-sufficient
        (i.e. dependent on the owning PIN object)."""
        def __init__(self, pPIN, pProperty, *args):
            if pPIN == None or pProperty == None or not isinstance(pProperty, (str, unicode)):
                raise InvalidParameter()
            self.mPIN = pPIN
            self.mProperty = pProperty
             # Note: It is assumed that we never need to command persistent updates, or manage the 'extras', at initialization.
            if isinstance(args[0], (list, tuple)):
                self.mList = list(args[0])
            else:
                self.mList = list(args)
        def __cmp__(self, pOther):
            if isinstance(pOther, (list, tuple)):
                return cmp(self.mList, pOther)
            elif isinstance(pOther, PIN.Collection):
                return cmp(self.mList, pOther.mList)
            raise InvalidParameter()
        def __repr__(self):
            return repr(self.mList)
        def __len__(self):
            return len(self.mList)
        def __getitem__(self, i):
            return self.mList[i]
        def __delitem__(self, i):
            if self.mPIN.mPID:
                # Grab the eid, and remove the corresponding 'extra'.
                if self.mPIN.mExtras.has_key(self.mProperty):
                    lExtras = self.mPIN.mExtras[self.mProperty]
                    if isInteger(i):
                        if i < 0 or len(lExtras) <= i:
                            raise Exception("i out of range: %d (%d elements)" % (i, len(lExtras)))
                        lEid = lExtras[i].mEid
                        del lExtras[i]
                        # Record a persistent update.
                        self.mPIN._handlePINUpdate(PIN({PIN.SK_PID:self.mPIN.mPID, self.mProperty:(0, PIN.Extra(pOp=mvstore_pb2.Value.OP_DELETE, pEid=lEid))}))
                    elif isinstance(i, slice):
                        # TODO: range checks
                        lEids = [iE.mEid for iE in lExtras[i]]
                        del lExtras[i]
                        # Record a persistent update.
                        for iEid in lEids:
                            self.mPIN._handlePINUpdate(PIN({PIN.SK_PID:self.mPIN.mPID, self.mProperty:(0, PIN.Extra(pOp=mvstore_pb2.Value.OP_DELETE, pEid=iEid))}))
                    else:
                        raise InvalidParameter("unexpected type for i: %s" % type(i))
            # Modify the list itself.
            del self.mList[i]
        def __setitem__(self, i, v):
            # Record a persistent update.
            if self.mPIN.mPID and self.mPIN.mExtras.has_key(self.mProperty):
                lExtras = self.mPIN.mExtras[self.mProperty]
                if isInteger(i):
                    if i < 0 or len(lExtras) <= i:
                        raise Exception("i out of range: %d (%d elements)" % (i, len(lExtras)))
                    lEid = lExtras[i].mEid                  
                    self.mPIN._handlePINUpdate(PIN({PIN.SK_PID:self.mPIN.mPID, self.mProperty:(v, PIN.Extra(pEid=lEid))}))
                # TODO: i can probably be a list
                else:
                    raise InvalidParameter("unexpected type for i: %s" % type(i))
            # Modify the list itself.
            self.mList[i] = v
        def insert(self, i, v):
            if self.mPIN.mPID:
                # Grab the eid, and insert the corresponding 'extra'.
                if self.mPIN.mExtras.has_key(self.mProperty):
                    lExtras = self.mPIN.mExtras[self.mProperty]
                    if isInteger(i):
                        if i < 0 or len(lExtras) < i:
                            raise Exception("i out of range: %d (%d elements)" % (i, len(lExtras)))
                        if isinstance(v, (list, tuple)): # Review: accept a PIN.Extra here?
                            raise Exception
                        lEid = EID_LAST_ELEMENT; lOp = mvstore_pb2.Value.OP_ADD
                        if i < len(lExtras):
                            lEid = lExtras[i].mEid; lOp = mvstore_pb2.Value.OP_ADD_BEFORE
                        lExtras.insert(i, PIN.Extra()) # Review: Can do better?
                        # Record a persistent update.
                        self.mPIN._handlePINUpdate(PIN({PIN.SK_PID:self.mPIN.mPID, self.mProperty:(v, PIN.Extra(pOp=lOp, pEid=lEid))}))
                    # TODO: i can probably be a list
                    else:
                        raise InvalidParameter("unexpected type for i: %s" % type(i))
            # Modify the list itself.
            self.mList.insert(i, v)
        def sort(self, cmp=cmp, key=None, reverse=False):
            # More for fun than anything; demonstrates ease of use.
            # Collect the list of values to sort.
            lToSort = [(self.mList[i], self.mPIN.getExtra(self.mProperty, pEpos=i).mEid, i) for i in xrange(len(self.mList))]
            # Sort them in memory.
            lToSort.sort(lambda x,y: cmp(x[0], y[0]), key, reverse)
            # Apply that ordering to the persistent state (in the context of the current transaction).
            lExtras = self.mPIN.mExtras[self.mProperty]
            lPrevEid = EID_FIRST_ELEMENT
            for i, iTuple in zip(xrange(len(lToSort)), lToSort):
                if lExtras[i].mEid == iTuple[1]:
                    # This element is already at the right place - don't touch it.
                    lPrevEid = iTuple[1]
                    continue
                if iTuple[2] <= i:
                    raise Exception
                # Move the element in memory, first.
                lV = self.mList.pop(iTuple[2])
                self.mList.insert(i, lV)
                lE = lExtras.pop(iTuple[2])
                lExtras.insert(i, lE)
                # Record a persistent update.
                lOp = (mvstore_pb2.Value.OP_MOVE_BEFORE, mvstore_pb2.Value.OP_MOVE)[lPrevEid != EID_FIRST_ELEMENT]
                self.mPIN._handlePINUpdate(PIN({PIN.SK_PID:self.mPIN.mPID, self.mProperty:(lPrevEid, PIN.Extra(pType=mvstore_pb2.Value.VT_UINT, pOp=lOp, pEid=iTuple[1]))}))
                lPrevEid = iTuple[1]
        def __str__(self):
            return str(self.mList)
    #--------
    # PUBLIC: Extra class.
    #--------
    class Extra(object):
        """Semi-hidden representation of all the mvStore adornments on a plain value (e.g. eid, meta, type, op, etc.).
        This allows to present the PIN as a simple dictionary where keys are property names, and values are native python values.
        Everything else is hidden as 'extras', and used mostly transparently when needed."""
        OP_NAMES = \
            ("OP_SET", "OP_ADD", "OP_ADD_BEFORE", "OP_MOVE", "OP_MOVE_BEFORE", "OP_DELETE", "OP_EDIT", "OP_RENAME", \
             "OP_PLUS", "OP_MINUS", "OP_MUL", "OP_DIV", "OP_MOD", "OP_NEG", "OP_NOT", "OP_AND", "OP_OR", "OP_XOR", \
             "OP_LSHIFT", "OP_RSHIFT", "OP_MIN", "OP_MAX", "OP_ABS", "OP_LN", "OP_EXP", "OP_POW", "OP_SQRT", \
             "OP_FLOOR", "OP_CEIL", "OP_CONCAT", "OP_LOWER", "OP_UPPER", "OP_TONUM", \
             "OP_TOINUM", "OP_CAST") # Review: could this be done with introspection?
        VT_NAMES = \
            ("VT_ANY", \
             "VT_INT", "VT_UINT", "VT_INT64", "VT_UINT64", \
             "VT_DECIMAL", "VT_FLOAT", "VT_DOUBLE", "VT_BOOL", \
             "VT_DATETIME", "VT_INTERVAL", \
             "VT_URIID", "VT_IDENTITY", \
             "VT_STRING", "VT_BSTR", "VT_URL", "VT_ENUM", \
             "[undefined-17]", \
             "VT_REFID", "[undefined-19]", "VT_REFIDPROP", "[undefined-21]", "VT_REFIDELT", "VT_EXPR", "VT_QUERY", \
             "VT_ARRAY", "[undefined-26]", "VT_STRUCT", "VT_RANGE", "[undefined-29]", "VT_CURRENT", "VT_REFCID") # Review: can this be done with introspection?
        def __init__(self, pPropID=None, pType=mvstore_pb2.Value.VT_ANY, pOp=mvstore_pb2.Value.OP_SET, pEid=EID_COLLECTION, pMeta=0):
            self.mPropID = pPropID # Conceptually redundant with the key, but kept for efficiency, since mvstore doesn't require a StringMap for existing propids.
            self.mType = pType # There are cases where a single native python value type covers multiple mvstore VT types... so we keep the actual specific type for future updates.
            self.mOp = pOp
            self.mEid = pEid
            self.mMeta = pMeta
        def __repr__(self):
            return "%s:%s:%x" % (PIN.Extra.OP_NAMES[self.mOp], PIN.Extra.VT_NAMES[self.mType], self.mEid)
        @classmethod
        def createFromPB(cls, pPBValue):
            "Create a 'Extra' instance for the specified mvstore_pb2.Value."
            return PIN.Extra(pPropID=pPBValue.property, pType=pPBValue.type, pOp=pPBValue.op, pEid=pPBValue.eid, pMeta=pPBValue.meta)
    #--------
    # PUBLIC: PID & Ref classes.
    #--------
    class PID(object):
        """PID native (non-PB) representation."""
        def __init__(self, pLocalPID, pIdent=0):
            if not isInteger(pLocalPID) or not isInteger(pIdent):
                raise InvalidParameter()
            self.mLocalPID = pLocalPID # 64-bit unsigned integer.
            self.mIdent = pIdent # 32-bit unsigned integer.
        def __eq__(self, pOther):
            if None == pOther:
                return False
            lOther = pOther
            if isinstance(lOther, str):
                return self.__repr__() == lOther
            if isinstance(lOther, PIN):
                lOther = lOther.mPID
            if self.mLocalPID != lOther.mLocalPID:
                return False
            if self.mIdent != lOther.mIdent:
                return False
            return True
        def __repr__(self):
            return "@%x" % self.mLocalPID
        @classmethod
        def fromPB(cls, pPBPID):
            return PIN.PID(pPBPID.id, pPBPID.ident)
    class Ref(PID):
        """PIN reference native (non-PB) representation."""
        def __init__(self, pLocalPID, pIdent=0, pProperty=None, pEid=None):
            super(PIN.Ref, self).__init__(pLocalPID, pIdent)
            self.mProperty = pProperty # Property name, or None.
            self.mEid = pEid # 32-bit unsigned integer, or None.
        def __eq__(self, pOther):
            if None == pOther:
                return False
            lOther = pOther
            if isinstance(lOther, str):
                return self.__repr__() == lOther
            if isinstance(lOther, PIN):
                lOther = lOther.mPID
            if self.mLocalPID != lOther.mLocalPID:
                return False
            if self.mIdent != lOther.mIdent:
                return False
            if self.mProperty != None:
                if not hasattr(lOther, 'mProperty'):
                    return False
                if self.mProperty != lOther.mProperty:
                    return False
            if self.mEid != None:
                if not hasattr(lOther, 'mEid'):
                    return False
                if self.mEid != lOther.mEid:
                    return False
            return True
        def __repr__(self):
            if self.mEid != None:
                return "@%x.%s[%d]" % (self.mLocalPID, self.mProperty, self.mEid)
            if self.mProperty != None:
                return "@%x.%s" % (self.mLocalPID, self.mProperty)
            return super(PIN.Ref, self).__repr__()
        @classmethod
        def fromPID(cls, pPID):
            return PIN.Ref(pPID.mLocalPID, pPID.mIdent)
    #--------
    # PUBLIC: Url class.
    #--------
    class Url(str):
        """To distinguish a URI from a plain string."""
        def __new__(self, value):
            return str.__new__(self, value)
        def __init__(self, value):
            pass
    #--------
    # PUBLIC: PIN update descriptor.
    #--------
    class PINUpdate(object):
        """Special marker on PIN objects, to identify that they represent updates, not the full PIN.
        Also allows to track other in-memory instances, and update them according to the changes effected by this update.
        Currently, we only support a single 'other' PIN, and only for eid inserts."""
        def __init__(self, pOtherPINs):
            self.mOtherPINs = pOtherPINs
    #--------
    # PUBLIC: Core overrides for dict base class.
    #--------
    def __init__(self, *args, **kwargs):
        "Constructor. Extracts the 'PID' argument, if specified, and redirects initialization of properties to the 'update' method."
        def _assignPID(_pPID):
            if not isinstance(_pPID, PIN.PID):
                raise InvalidParameter()
            self.mPID = _pPID
        def _getSpecialValue(_pSpecialKey):
            if _pSpecialKey in kwargs.keys():
                return kwargs[_pSpecialKey]
            if len(args) > 0 and isinstance(args[0], dict) and _pSpecialKey in args[0].keys():
                return args[0][_pSpecialKey]
            return None
        self.mExtras = {} # Same layout as the main dict, but the values are instances of Extra.
        self.mPID = None
        self.update(*args, **kwargs)
        # Assign the PID after we update, to distinguish initialization from actual updates (in __setitem__),
        # and avoid an infinite recursion of save-create-save-create...
        # Review: to further reduce risk of collision with real properties, could use special (e.g. numeric/custom class) keys...
        lPID = _getSpecialValue(PIN.SK_PID)
        if lPID:
            _assignPID(lPID)
        self.mIsUpdate = _getSpecialValue(PIN.SK_UPDATE)
        if self.mIsUpdate and not isinstance(self.mIsUpdate, PIN.PINUpdate):
            raise InvalidParameter()
    def __setitem__(self, pKey, pValue):
        "Override of the dict implementation, to extract the optional 'Extra' specification and store it separately."
        if isinstance(pValue, (list, tuple, MutableSequence)):          
            if 2 == len(pValue) and isinstance(pValue[1], PIN.Extra):
                super(PIN, self).__setitem__(pKey, pValue[0])
                self.mExtras[pKey] = [pValue[1]]
            elif 1 <= len(pValue) and isinstance(pValue[0], (list, tuple, MutableSequence)):
                if isinstance(pValue[0][1], PIN.Extra):
                    super(PIN, self).__setitem__(pKey, PIN.Collection(self, pKey, [iV[0] for iV in pValue]))
                    self.mExtras[pKey] = [iV[1] for iV in pValue]
                else:
                    raise InvalidParameter()
            else:
                super(PIN, self).__setitem__(pKey, PIN.Collection(self, pKey, pValue))
                lExtras = [PIN.Extra()]
                for i in xrange(len(pValue) - 1):
                    lExtras.append(PIN.Extra(pOp=mvstore_pb2.Value.OP_ADD, pEid=EID_LAST_ELEMENT))
                self.mExtras[pKey] = lExtras # Simplifies things by guarantying that there are 'extras' everywhere... but may want to refine this.
        else:
            super(PIN, self).__setitem__(pKey, pValue)
            self.mExtras[pKey] = [PIN.Extra()] # Simplifies things by guarantying that there are 'extras' everywhere... but may want to refine this.
        if self.mPID != None:
            self._handlePINUpdate(PIN({PIN.SK_PID:self.mPID, pKey:pValue}))
    def __delitem__(self, pKey):
        "Override of the dict implementation, to cleanup the corresponding 'Extra' items."
        super(PIN, self).__delitem__(pKey)
        if self.mExtras.has_key(pKey):
            del self.mExtras[pKey]
        if self.mPID != None:
            self._handlePINUpdate(PIN({PIN.SK_PID:self.mPID, pKey:(0, PIN.Extra(pOp=mvstore_pb2.Value.OP_DELETE))}))
    def update(self, *args, **kwargs):
        "Override of the dict implementation, to extract the optional 'Extra' specifications and store them separately."
        def _assign(_pDict):
            for _lKey in _pDict:
                if _lKey not in (PIN.SK_PID, PIN.SK_UPDATE):
                    self[_lKey] = _pDict[_lKey]
        # 'update' accepts a positional argument that must be a dictionary.
        if args:
            if len(args) > 1:
                raise TypeError("PIN::update expected at most 1 positional argument, got %d" % len(args))
            if not isinstance(args[0], dict):
                raise TypeError("PIN::update expected at the positional argument to be a dict, not a %s" % type(args[0]))
            lOther = dict(args[0])
            _assign(lOther)
        # 'update' also accepts keyword arguments, where the argument name is used as a key, and the value of the argument as the corresponding value.
        _assign(kwargs)
    def __repr__(self):
        lRepr = "PIN " + repr(self.mPID) + " {\n"
        lRepr += "  data: " + super(PIN, self).__repr__() + "\n"
        lRepr += "  extras: " + repr(self.mExtras) + "\n"
        lRepr += "}"
        return lRepr
    #--------
    # PUBLIC: Direct access to 'Extras'.
    #--------
    def getExtra(self, pProperty, pEid=EID_COLLECTION, pEpos=-1):
        "Return the 'Extra' structure associated with the value specified by pProperty and either pEid or pEpos."
        if self.mExtras.has_key(pProperty):
            lExtras = self.mExtras[pProperty]
            if pEpos >= 0 and pEpos < len(lExtras):
                return lExtras[pEpos]
            elif pEid != EID_COLLECTION:
                for iV in lExtras:
                    if iV.mEid == pEid:
                        return iV
            elif 1 == len(lExtras):
                return lExtras[0]
            else:
                raise InvalidParameter("Invalid pEid or pEpos")
        raise InvalidParameter("Invalid property: %s" % pProperty)
    def setExtra(self, pProperty, pExtra, pEid=EID_COLLECTION, pEpos=-1):
        "Replace the 'Extra' structure associated with the value specified by pProperty and either pEid or pEpos, with pExtra."
        if self.mExtras.has_key(pProperty):
            lExtras = self.mExtras[pProperty]
            if pEpos >= 0 and pEpos < len(lExtras):
                lExtras[pEpos] = copy(pExtra)
                return
            elif pEid != EID_COLLECTION:
                for i, iV in zip(xrange(len(lExtras)), lExtras):
                    if iV.mEid == pEid:
                        lExtras[i] = copy(pExtra)
                        return
            elif 1 == len(lExtras):
                lExtras[0] = copy(pExtra)
        raise InvalidParameter()
    def markAsUpdate(self, pUpdate):
        if not isinstance(pUpdate, PIN.PINUpdate):
            raise InvalidParameter()
        self.mIsUpdate = pUpdate
    #--------
    # PUBLIC: PIN saving (to mvStore). Note: At this level of the API, a PIN can represent a whole mvStore PIN, or just a set of updates to be applied on an actual mvStore PIN.
    #--------
    @classmethod
    def _savePINsi(cls, pPINs, pTxCtx):
        "[internal] Core  implementation of savePINs."
        def _insertsCollectionElements(_pPBPin):
            for _iV in _pPBPin.values:
                if _iV.op in (mvstore_pb2.Value.OP_ADD, mvstore_pb2.Value.OP_ADD_BEFORE):
                    return True
            return False
        if 0 == len(pPINs) or None == pTxCtx:
            raise InvalidParameter()
        # Prepare the StringMap of properties.
        lPropDictLen = len(pTxCtx.mPropDict.keys())
        for iPin in pPINs:
            iPin._preparePBPropIDs(pTxCtx)
        if len(pTxCtx.mPropDict.keys()) > lPropDictLen:
            pTxCtx.capture()
        # Serialize the PINs.
        for iPin in pPINs:
            lPBPin = pTxCtx.getPBStream().pins.add()
            if iPin.mPID != None:
                lPBPin.op = mvstore_pb2.MVStream.OP_UPDATE
                lPBPin.id.id = iPin.mPID.mLocalPID
                lPBPin.id.ident = iPin.mPID.mIdent
            else:
                lPBPin.op = mvstore_pb2.MVStream.OP_INSERT
            iPin._preparePBValues(pTxCtx, lPBPin)
            lPBPin.rtt = (mvstore_pb2.RT_PIDS, mvstore_pb2.RT_PINS)[_insertsCollectionElements(lPBPin)]
            #print ("requested rtt=%s" % lPBPin.rtt)
            lPBPin.nValues = len(lPBPin.values)
        if len(pPINs) > 0:
            pTxCtx.capture()        
    @classmethod
    def savePINs(cls, pPINs, pTxCtx=None):
        "Save pPINs to the store."
        if 0 == len(pPINs):
            return
        # Serialize, and request an immediate response (synchronous reception of resulting PIDs, eids etc.).
        lTxCtx = pTxCtx or MVSTORE()._txCtx()
        try:
            cls._savePINsi(pPINs, lTxCtx)
            lTxCtx.flush()
        except Exception as ex:
            traceback.print_exc()
            raise
        # Handle errors.
        if lTxCtx.mRC == None:
            logging.warn("failed to save PINs.")
            return None
        # Determine if we need to process the output.
        lProcessOutput = not lTxCtx.isOutputIgnored()
        if not lProcessOutput:
            for iP in pPINs:
                if iP.mIsUpdate and len(iP.mIsUpdate.mOtherPINs) > 0:
                    lProcessOutput = True
                    break
        if not lProcessOutput or not lTxCtx.mPBOutput:
            return pPINs
        # Obtain the resulting IDs generated by mvStore.
        if len(lTxCtx.mPBOutput.pins) != len(pPINs):
            logging.warn("%s PINs were saved, but response contained only %s PINs." % (len(pPINs), len(lTxCtx.mPBOutput.pins)))
        lReadCtx = PBReadCtx(lTxCtx.mPBOutput)
        for iPBPin, iPin in zip(lTxCtx.mPBOutput.pins, pPINs):
            # Substitute iPin if it's an update PIN on an identified actual PIN.
            # Review: lots of potential improvements and verifications...
            lPin = iPin
            if iPin.mIsUpdate and len(iPin.mIsUpdate.mOtherPINs) > 0:
                lPin = iPin.mIsUpdate.mOtherPINs[0]
            # The PID.
            if lPin.mPID != None:
                if lPin.mPID.mLocalPID != iPBPin.id.id:
                    raise Exception("Expected PID %s but received %s" % (lPin.mPID.mLocalPID, iPBPin.id.id))
            else:
                lPin.mPID = PIN.PID(pLocalPID=iPBPin.id.id, pIdent=iPBPin.id.ident)
            # The eids.
            # TODO: review if in Sonny's java impl, he took care of patching the resulting eids...
            for iV in iPBPin.values:
                lPN = lReadCtx.getPropName(iV.property)
                lExtra = lPin.mExtras[lPN]
                if mvstore_pb2.Value.VT_ARRAY == iV.type:
                    for i, iE in zip(xrange(iV.varray.l), iV.varray.v): # Review: always true?
                        if (lExtra[i].mEid in (EID_COLLECTION, EID_LAST_ELEMENT, EID_FIRST_ELEMENT)) or (lExtra[i].mOp in (mvstore_pb2.Value.OP_ADD, mvstore_pb2.Value.OP_ADD_BEFORE)):
                            lExtra[i].mEid = iE.eid
                            logging.debug("obtained eid=%s (%s)" % (iE.eid, lPN))
                elif lExtra[0].mOp in (mvstore_pb2.Value.OP_ADD, mvstore_pb2.Value.OP_ADD_BEFORE):
                    lExtra[0].mEid = iV.eid
                    logging.debug("obtained eid=%s (%s)" % (iV.eid, lPN))
                else:
                    logging.info("didn't obtain eid (%s): %s" % (lPN, iV))
        lTxCtx.mPBOutput = None
        return pPINs
    def savePIN(self, pTxCtx=None):
        "Save self to the store."
        logging.debug("saving %s" % repr(self))
        PIN.savePINs([self], pTxCtx)
        return self
    #--------
    # PUBLIC: Helpers for PIN modifications.
    #--------
    def addElement(self, pProperty, pValue, pAlwaysCollection=True):
        "Add a value for pProperty; if pProperty doesn't exist yet, create it (if pAlwaysCollection, create a 1-element collection)."
        if self.has_key(pProperty):
            if isinstance(self[pProperty], PIN.Collection):
                self[pProperty].append(pValue)
            else:
                self[pProperty] = ((self[pProperty], self.getExtra(pProperty)), (pValue, PIN.Extra(pOp=mvstore_pb2.Value.OP_ADD, pEid=EID_LAST_ELEMENT)))
        elif pAlwaysCollection:
            self[pProperty] = (pValue, )
        else:
            self[pProperty] = pValue
    def moveXafterY(self, pProperty, pXpos, pYpos, pTxCtx=None):
        "In the collection specified by pProperty, move the element at index pXpos after the element at index pYpos."
        return self._moveXvsY(pProperty, pXpos, pYpos, pAfter=True, pTxCtx=pTxCtx)
    def moveXbeforeY(self, pProperty, pXpos, pYpos, pTxCtx=None):
        "In the collection specified by pProperty, move the element at index pXpos before the element at index pYpos."
        return self._moveXvsY(pProperty, pXpos, pYpos, pAfter=False, pTxCtx=pTxCtx)
    #--------
    # PUBLIC: PIN deletion (from mvStore).
    #--------
    # TODO: soft vs purge, undelete etc.
    @classmethod
    def deletePINs(cls, pPIDs, pTxCtx=None):
        "Delete pPIDs from the store."
        lTxCtx = pTxCtx or MVSTORE()._txCtx()
        for iPid in pPIDs:
            if not isinstance(iPid, PIN.PID):
                raise InvalidParameter()
            lPBPin = lTxCtx.getPBStream().pins.add()
            lPBPin.id.id = iPid.mLocalPID
            lPBPin.id.ident = iPid.mIdent
            lPBPin.op = mvstore_pb2.MVStream.OP_DELETE
        lTxCtx.flush(pExplicit=False)
        # TODO: Decide if final confirmation is a RC or an exception.
    def deletePIN(self, pTxCtx=None):
        "Delete self from the store."
        if None == self.mPID:
            raise InvalidParameter()
        PIN.deletePINs((self.mPID, ), pTxCtx)
    #--------
    # PUBLIC: PIN loading/refreshing (from mvStore).
    #--------
    @classmethod
    def loadPINs(cls, pPBStream):
        "Load all PINs present in the protobuf stream, and return them."
        lReadCtx = PBReadCtx(pPBStream)
        return [PIN().loadPIN(lReadCtx, iPBPin) for iPBPin in lReadCtx.getPBStream().pins]
    @classmethod
    def createFromPID(cls, pPID):
        "Load the PIN specified by pPID, and return it as a new PIN object."
        lPBStream = MVSTORE().mvsqlProto("SELECT * FROM {@%x};" % pPID.mLocalPID)
        return PIN().loadPIN(PBReadCtx(lPBStream), lPBStream.pins[0])
    def loadPIN(self, pReadCtx, pPBPin):
        "Load the specified PIN in-place."
        self._clearPIN()
        for iV in pPBPin.values:
            lPropName = pReadCtx.getPropName(iV.property)
            self[lPropName] = PIN._valuePB2PY(pReadCtx, iV)
        # Assign last, to avoid unwanted persistent update requests.
        self.mPID = PIN.PID(pLocalPID=pPBPin.id.id, pIdent=pPBPin.id.ident)
        return self
    def refreshPIN(self):
        "Refresh the contents of self, by rereading the whole PIN from the store."
        if None == self.mPID:
            return self
        lPBStream = MVSTORE().mvsqlProto("SELECT * FROM {@%x};" % self.mPID.mLocalPID)
        return self.loadPIN(PBReadCtx(lPBStream), lPBStream.pins[0])
    def _clearPIN(self):
        "Clear the contents of self."
        self.mPID = None # Assign first, since the goal is not to clear the persisted PIN.
        self.clear()
        self.mExtras.clear()
    #---------
    # PRIVATE: Conversions between mvstoree_pb2.Value and our python native representation of values.
    #---------
    @staticmethod
    def _valuePY2PB(pPBValue, pPYValue, pPropDict):
        "[internal] Convert a native python value (pPYValue) into a mvstore_pb2.Value. If pPYValue is a tuple containing an 'Extra' description, use every available field."
        lType = pPBValue.type
        pPBValue.type = mvstore_pb2.Value.VT_ANY
        if isinstance(pPYValue, PIN.Url):
            pPBValue.str = pPYValue
            lType = mvstore_pb2.Value.VT_URL
        elif isinstance(pPYValue, str):
            pPBValue.str = pPYValue
            lType = mvstore_pb2.Value.VT_STRING
        elif isinstance(pPYValue, unicode):
            pPBValue.str = pPYValue
            lType = mvstore_pb2.Value.VT_STRING
        elif isinstance(pPYValue, bytearray):
            pPBValue.bstr = str(pPYValue)
            lType = mvstore_pb2.Value.VT_BSTR
        elif isinstance(pPYValue, bool):
            pPBValue.b = pPYValue
            lType = mvstore_pb2.Value.VT_BOOL
        elif isinstance(pPYValue, (int, long)):
            # If we already know the type, don't mess with it.
            # Review: There might be cases where the new value is voluntarily not compatible; for now, expect explicit type spec in such case.
            if lType == mvstore_pb2.Value.VT_INT:
                pPBValue.i = pPYValue
            elif lType == mvstore_pb2.Value.VT_UINT:
                pPBValue.ui = pPYValue
            elif lType == mvstore_pb2.Value.VT_INT64:
                pPBValue.i64 = pPYValue
            elif lType == mvstore_pb2.Value.VT_UINT64:
                pPBValue.ui64 = pPYValue
            # Otherwise, guess.
            elif (pPYValue >= -2147483648 and pPYValue <= 2147483647):
                pPBValue.i = pPYValue
                lType = mvstore_pb2.Value.VT_INT
            elif pPYValue >= 0 and pPYValue <= 4294967295:
                pPBValue.ui = pPYValue
                lType = mvstore_pb2.Value.VT_UINT
            elif pPYValue <= 9223372036854775807:
                pPBValue.i64 = pPYValue
                lType = mvstore_pb2.Value.VT_INT64
            else:
                pPBValue.ui64 = pPYValue
                lType = mvstore_pb2.Value.VT_UINT64
        elif isinstance(pPYValue, float):
            if lType == mvstore_pb2.Value.VT_FLOAT:
                pPBValue.f = pPYValue
            else:
                pPBValue.d = pPYValue
                lType = mvstore_pb2.Value.VT_DOUBLE
        elif isinstance(pPYValue, PIN.Ref):
            if pPYValue.mEid != None:
                pPBValue.ref.id.id = pPYValue.mLocalPID
                pPBValue.ref.id.ident = pPYValue.mIdent
                pPBValue.ref.property = pPropDict.get(pPYValue.mProperty)
                pPBValue.ref.eid = pPYValue.mEid
                lType = mvstore_pb2.Value.VT_REFIDELT
            elif pPYValue.mProperty != None:
                pPBValue.ref.id.id = pPYValue.mLocalPID
                pPBValue.ref.id.ident = pPYValue.mIdent
                pPBValue.ref.property = pPropDict.get(pPYValue.mProperty)
                lType = mvstore_pb2.Value.VT_REFIDPROP
            else:
                pPBValue.id.id = pPYValue.mLocalPID
                pPBValue.id.ident = pPYValue.mIdent            
                lType = mvstore_pb2.Value.VT_REFID
        elif isinstance(pPYValue, datetime.datetime):
            # Review: Probably shouldn't need to deal with {mvstore, timezone, dst} conversion on client side. 
            lTT = list(pPYValue.timetuple()); lTT[8] = 0
            pPBValue.datetime = int(1000000.0 * time.mktime(lTT) + pPYValue.microsecond + PIN.TIME_OFFSET)
            lType = mvstore_pb2.Value.VT_DATETIME
        elif isinstance(pPYValue, datetime.timedelta):
            pPBValue.interval = int(pPYValue.days*86400000000.0 + pPYValue.seconds*1000000.0 + pPYValue.microseconds)
            lType = mvstore_pb2.Value.VT_INTERVAL
        else:
            logging.warn("Value type not yet supported: %s (%s)" % (type(pPYValue), pPYValue))
        if pPBValue.type == mvstore_pb2.Value.VT_ANY:
            pPBValue.type = lType
    @staticmethod
    def _valuePB2PY(pPBReadCtx, pPBValue):
        "[internal] Convert a mvstore_pb2.Value into either a single (native python value, Extra), or a list of them (if pPBValue is a collection), and return it."
        if (pPBValue.type == mvstore_pb2.Value.VT_ARRAY):
            lResult = []
            for iV in pPBValue.varray.v:
                lResult.append(PIN._valuePB2PY(pPBReadCtx, iV))
            return lResult
        lExtra = PIN.Extra.createFromPB(pPBValue)
        if (pPBValue.type == mvstore_pb2.Value.VT_URL):
            return (PIN.Url(pPBValue.str), lExtra)
        elif (pPBValue.type == mvstore_pb2.Value.VT_STRING):
            return (str(pPBValue.str), lExtra)
        elif (pPBValue.type == mvstore_pb2.Value.VT_BSTR):
            return (bytearray(pPBValue.bstr), lExtra)
        elif (pPBValue.type == mvstore_pb2.Value.VT_INT):
            return (pPBValue.i, lExtra)
        elif (pPBValue.type == mvstore_pb2.Value.VT_UINT):
            return (pPBValue.ui, lExtra)
        elif (pPBValue.type == mvstore_pb2.Value.VT_INT64):
            return (pPBValue.i64, lExtra)
        elif (pPBValue.type == mvstore_pb2.Value.VT_UINT64):
            return (pPBValue.ui64, lExtra)
        elif (pPBValue.type == mvstore_pb2.Value.VT_FLOAT):
            return (pPBValue.f, lExtra)
        elif (pPBValue.type == mvstore_pb2.Value.VT_DOUBLE):
            return (pPBValue.d, lExtra)
        elif (pPBValue.type == mvstore_pb2.Value.VT_INTERVAL):
            return (datetime.timedelta(microseconds=pPBValue.interval), lExtra)
        elif (pPBValue.type == mvstore_pb2.Value.VT_BOOL):
            return ((False, True)[pPBValue.b != 0], lExtra)
        elif (pPBValue.type == mvstore_pb2.Value.VT_DATETIME):
            # Review: Probably shouldn't need to deal with {mvstore, timezone, dst} conversion on client side. 
            lMsInS = float(pPBValue.datetime % 1000000) / 1000000.0
            lTT = list(time.gmtime(float(pPBValue.datetime - PIN.TIME_OFFSET) / 1000000.0 - time.timezone)); lTT[8] = -1
            return (datetime.datetime.fromtimestamp(time.mktime(lTT) + lMsInS), lExtra)
        elif (pPBValue.type == mvstore_pb2.Value.VT_QUERY):
            return (pPBValue.str, lExtra)
        elif (pPBValue.type == mvstore_pb2.Value.VT_REFID):
            return (PIN.Ref(pLocalPID=pPBValue.id.id, pIdent=pPBValue.id.ident), lExtra) 
        elif (pPBValue.type == mvstore_pb2.Value.VT_REFIDPROP):
            return (PIN.Ref(pLocalPID=pPBValue.ref.id.id, pIdent=pPBValue.ref.id.ident, pProperty=pPBReadCtx.getPropName(pPBValue.ref.property)), lExtra) 
        elif (pPBValue.type == mvstore_pb2.Value.VT_REFIDELT):
            return (PIN.Ref(pLocalPID=pPBValue.ref.id.id, pIdent=pPBValue.ref.id.ident, pProperty=pPBReadCtx.getPropName(pPBValue.ref.property), pEid=pPBValue.ref.eid), lExtra) 
        elif (pPBValue.type == mvstore_pb2.Value.VT_URIID):
            lV = pPBReadCtx.getPropName(pPBValue.ui)
            if lV == None:
                lV = pPBValue.ui
                logging.warn("Could not resolve VT_URIID %s" % pPBValue.ui)
            return (lV, lExtra)
        logging.warn("Unknown value type %s" % pPBValue.type)
        return None
    #---------
    # PRIVATE: Preparation steps for protobuf streams sent to mvStore.
    #---------
    def _preparePBPropIDs(self, pTxCtx):
        "[internal] Extract the StringMap of this PIN's properties, and merge it into pTxCtx.mPropDict."
        def __prepid(_pPropName):
            if not pTxCtx.mPropDict.has_key(_pPropName):
                _lPBProp = pTxCtx.getPBStream().properties.add()
                _lPBProp.str = _pPropName
                _lPBProp.id = (mvstore_pb2.SP_MAX + 1) + len(pTxCtx.mPropDict)
                pTxCtx.mPropDict[_lPBProp.str] = _lPBProp.id
        def __preprefid(_pValue):
            if isinstance(_pValue, PIN.Ref) and _pValue.mProperty != None:
                __prepid(_pValue.mProperty)
        # Add all the PIN's properties to the StringMap.
        # Review: Use the mPropID of mExtras, when possible... maybe...
        for iPropName in self.iterkeys():
            __prepid(iPropName)
        # If there are property references, account for them.
        for iPV in self.iteritems():
            if isinstance(iPV[1], (list, tuple, MutableSequence)):
                for iE in iPV[1]:
                    __preprefid(iE)
            else:
                __preprefid(iPV[1])
    def _preparePBValues(self, pTxCtx, pPBPin):
        "[internal] Add mvstore_pb2.Value objects to pPBPin, representing self.items()."
        def __prep(_pPropName, _pExtra, _pPYVal):
            _lV = pPBPin.values.add()
            _lV.property = 0
            if _pExtra.mPropID != None:
                _lV.property = _pExtra.mPropID
            if _pExtra.mType != None:
                _lV.type = _pExtra.mType
            _lV.op = _pExtra.mOp
            _lV.eid = _pExtra.mEid
            _lV.meta = _pExtra.mMeta
            PIN._valuePY2PB(_lV, _pPYVal, pTxCtx.mPropDict)
            if 0 == _lV.property:
                _lV.property = pTxCtx.mPropDict[_pPropName]
        for iPV, iExtra in zip(self.iteritems(), self.mExtras.itervalues()):
            # Collection.
            if isinstance(iPV[1], (list, tuple, MutableSequence)):
                if not isinstance(iExtra, (list, tuple)):
                    raise Exception
                for i, iE in zip(xrange(len(iPV[1])), iPV[1]):
                    __prep(iPV[0], iExtra[i], iE)
            # Scalar.
            else:
                __prep(iPV[0], iExtra[0], iPV[1])
    #---------
    # PRIVATE: Implementation of persistent modifications to an existing PIN.
    #---------
    class _SilentChange(object):
        def __init__(self, pPIN):
            self.mPIN = pPIN
        def __enter__(self):
            self.mPID = self.mPIN.mPID
            self.mPIN.mPID = None
        def __exit__(self, etyp, einst, etb):
            self.mPIN.mPID = self.mPID
    def _handlePINUpdate(self, pPINUpdate, pTxCtx=None):
        lTxCtx = pTxCtx or MVSTORE().mTxCtx
        pPINUpdate.markAsUpdate(PIN.PINUpdate([self]))
        if lTxCtx and not lTxCtx.performImmediateUpdates() and 0 != lTxCtx.mTxCnt: # XXXXX check that equivalent...
            lTxCtx.recordPINUpdate(pPINUpdate)
        else:
            pPINUpdate.savePIN(lTxCtx)
    def _moveXvsY(self, pProperty, pXpos, pYpos, pAfter=True, pTxCtx=None):
        # Validate the arguments.
        if pXpos == pYpos:
            return
        if not self.has_key(pProperty) or not isinstance(self[pProperty], (list, tuple, MutableSequence)):
            raise InvalidParameter()
        if not isInteger(pXpos) or pXpos < 0 or pXpos > len(self[pProperty]):
            raise InvalidParameter()
        if not isInteger(pYpos) or pYpos < 0 or pYpos > len(self[pProperty]):
            raise InvalidParameter()
        # Perform the local (in-memory) modification.
        # Review: Should this also be abstracted?
        # Review: _SilentChange allows me to produce a more efficient persistent representation than the concatenation of insert+pop; but the mechanism used (setting mPID to None temporarily) is arguable.
        with PIN._SilentChange(self):
            lCollection = self[pProperty]
            lCollection.insert((pYpos, pYpos + 1)[pAfter], lCollection[pXpos])
            lCollection.pop((pXpos, pXpos + 1)[pXpos > pYpos])
        # Prepare and record/execute the persistent update.
        lE1 = self.getExtra(pProperty, pEpos=pXpos).mEid
        lE2 = self.getExtra(pProperty, pEpos=pYpos).mEid
        lOp = (mvstore_pb2.Value.OP_MOVE_BEFORE, mvstore_pb2.Value.OP_MOVE)[pAfter]
        self._handlePINUpdate(PIN({ \
            PIN.SK_PID:self.mPID, \
            pProperty:(lE2, PIN.Extra(pType=mvstore_pb2.Value.VT_UINT, pOp=lOp, pEid=lE1))}))

# TODO: document the effect of recordPINUpdate, and correct usage (e.g. since updates are deferred, queries can be affected)
# TODO: better del support, for the pin itself; better remove for last element of collection
# TODO: finish testing the more esoteric formats, to make sure their conversion is good
# TODO: test that units of measurement work ok back and forth between mvSQL and pure protobuf
# ---
# TODO: mark pins that have participated to a rolled-back transaction as dirty, and refresh them if reused
# TODO: make sure that all paths produce exceptions when necessary (e.g. all MVSTORE().get/post failures); show better in samples/tests
# TODO: could accept a PIN as a reference value (equivalent to PIN.Ref) - although it may open up hopes and questions that we may want to avoid
# TODO: could add mass moving helpers (for coll elements: move all in X=(...) before/after Y [in the order specified in X]).
# TODO: produce warnings/errors when (or deal with) a PIN with extras is manipulated in such a way that consistency between extras and values is lost.
# TODO: probably discard the notion of session in MVStoreConnection (it was desirable in a test environment, but here I feel it's more questionable).
# TODO: maybe implement __deepcopy__
# TODO: when available, offer the option to apply modifs only if the stamp didn't change
# TODO: support namespaces in protobuf mode?

# TODO: in PBTransactionCtx, introduce streaming, as an option to fetch the data as a cursor instead of as a MVStream. Probably a few more options as well: PB output, PIN output, PID output, cursor, ...
    #from google.protobuf.internal import decoder, encoder
    #def test_streaming(pPinIDs):
        ## write something like InternalParse
        #lRawRes = gMvStore.mvsql("SELECT * FROM {@%x};" % pPinIDs[0].id)
        #lStreamObj = mvstore_pb2.MVStream()
        ##lStreamRes.ParseFromString(lRawRes)
        ## Note:
        ##   I adapted the code below from protobuf/reflection.py:InternalParse,
        ##   which is the implementation of ParseFromString. This highlights the
        ##   required mechanisms to implement streaming of inner/repeated fields
        ##   for read, in python. I will be using this in the upcoming python
        ##   library.
        #lTag_pins = encoder.TagBytes(3, 2) # mvstore_pb2.MVStream.pins.number, length-delimited wire type
        #pos = 0
        #local_ReadTag = decoder.ReadTag
        #local_SkipField = decoder.SkipField
        #decoders_by_tag = lStreamObj.__class__._decoders_by_tag
        ##print decoders_by_tag
        #lFieldDict = lStreamObj._fields
        #while pos != len(lRawRes):
          #(tag_bytes, new_pos) = local_ReadTag(lRawRes, pos)          
          #field_decoder = decoders_by_tag.get(tag_bytes)
          #if tag_bytes == lTag_pins and False:
              #print "just for fun, skip the pins"
              #field_decoder = None
          #if field_decoder is None:
            #new_pos = local_SkipField(lRawRes, new_pos, len(lRawRes), tag_bytes)
            #if new_pos == -1:
              #return pos
            #pos = new_pos
          #else:
            #pos = field_decoder(lRawRes, new_pos, len(lRawRes), lStreamObj, lFieldDict)
        #if pos != len(lRawRes):
            #raise Exception
        #print "number of pins found via streaming: %d" % len(lStreamObj.pins)
        #if len(lStreamObj.pins) > 0:
            #lReader = MVStoreReader(lStreamObj)
            #lPinChk = lReader.readPIN(lStreamObj.pins[0])
            #print "pin read via streaming: %s" % lPinChk
