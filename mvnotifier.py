#!/usr/bin/env python2.6
"""This module provides access to mvStore's notification service. An application can request
notifications when specific PINs change, or when specific classes change (i.e. when PINs are
classified or un-classified, as well as when already classified PINs change). An application
can also emit standard queries to retrieve all changes since time x. The current implementation
uses a 'comet' pattern; in the future, a more efficient networking solution will be
implemented."""
from __future__ import with_statement
from mvstore import *
import copy
import json
import logging
try:
    import multiprocessing # Optional (for more flexible Event object).
    import mvstoreinproc # Optional (for inproc execution of mvstore; see the 'ext' subdirectory).
except:
    pass
import os
import socket
import threading
import urllib2
import uuid

# TODO: auth
# TODO: support qnames for class registration

class MVNotifier(object):
    """Client-side notification manager. Accepts registrations for specific PINs and classes,
    and invokes the specified handlers (callbacks) upon mvStore notifications."""
    class _ThreadCtx(object):
        """[internal] Thread context for the threads that wait for notifications."""
        def __init__(self, pCriterion, pHandler, pHandlerData, pCreateThread, pNotifier):
            self.mServerToken = None # The token returned at registration time by the server, to identify a specific notification channel.
            self.mCriterion = pCriterion # The criterion used at registration time (more 'human-readable' identifier of the notification).
            self.mHandler = pHandler # The handler/callback provided by the client, to be called upon notification.
            self.mHandlerData = pHandlerData # Additional data provided by the client, to be returned to him upon notification.
            try:
                self.mFinished = multiprocessing.Event() # allows Ctrl+C
            except:
                self.mFinished = threading.Event()
            self.mThread = None # Contexts attached to the "group" context don't have their own thread.
            if pCreateThread:
                self.mThread = threading.Thread(name="MVNotifier.%s" % pCriterion, target=MVNotifier._staticEntryPoint, args=(pNotifier, self))
                self.mThread.setDaemon(1) # to allow Ctrl+C
        def start(self, pServerToken):
            self.mServerToken = pServerToken
            if self.mThread != None:
                self.mThread.start()
        finished = property(fget=lambda c: (c.mFinished.__class__.__dict__.has_key("isSet") and c.mFinished.isSet()) or (c.mFinished.__class__.__dict__.has_key("is_set") and c.mFinished.is_set()), fset=lambda c,v: c.mFinished.set(), doc="Turns true when this thread is signaled to end.")
    def __init__(self):
        self.mDbConnection = None
        self.mClientID = "python:%s:%s" % (os.getpid(), uuid.uuid4().hex)
        self.mClassNames = {} # Classname-based registrations; the value is a notification handler callback.
        self.mPIDs = {} # PID-based registrations; the value is a notification handler callback.
        self.mTokens = {} # Mixed registrations, by server token.
        self.mLock = threading.Lock() # Synchronization, to allow unregistration from the callbacks.
        self.mPendingCtxs = set() # Threads currently waiting for a http response.
        self.mGroupCtx = None # Thread used for all notifications grouped by clientid.
        self.mMvstoreInproc = MVStoreConnection.isInproc()
    def open(self, pDbConnection):
        "Initializer for the notifier."
        if not isinstance(pDbConnection, MVStoreConnection):
            raise Exception("Invalid pDbConnection passed to MVNotifier.")
        self.mDbConnection = pDbConnection
        self.mGroupCtx = MVNotifier._ThreadCtx(self.mClientID, MVNotifier._groupHandler, self, True, self) # Thread allowing to group together all notifications.
        self.mGroupCtx.start(None)
    def close(self):
        "Terminator for the notifier."
        # Terminate the group ctx thread, and wait for completion.
        self.mGroupCtx.finished = True
        self.mGroupCtx.mThread.join()
        # Wait for completion of all pending threads.
        lPendingCtxs = None
        with self.mLock:
            lPendingCtxs = copy.copy(self.mPendingCtxs)
        for i in lPendingCtxs:
            i.mThread.join()
        # Report any dangling registration.
        for i in self.mClassNames.items():
            if len(i[1]) > 0:
                logging.warn("notification registration still active: %s" % i)
        for i in self.mPIDs.items():
            if len(i[1]) > 0:
                logging.warn("notification registration still active: %s" % i)
    def registerClass(self, pClassName, pHandler, pHandlerData=None, pGroupNotifs=True):
        "Registration of notifications for class-related changes."
        def _regci():
            if self.mMvstoreInproc: return mvstoreinproc.regnotif(self.mDbConnection._s(None), pClassName, None, self.mClientID)
            return self._callServer("http://%s/db/?i=regnotif&notifparam=%s&type=class&clientid=%s" % (self.mDbConnection.host(), pClassName, self.mClientID))
        lThreadCtx = MVNotifier._ThreadCtx(pClassName, pHandler, pHandlerData, not pGroupNotifs, self)
        lResult = json.loads(_regci())
        if lResult and len(lResult.keys()) > 0:
            with self.mLock:
                lToken = lResult.keys()[0]
                if not self.mClassNames.has_key(pClassName):
                    self.mClassNames[pClassName] = []
                self.mClassNames[pClassName].append(lThreadCtx)
                self.mTokens[lToken] = lThreadCtx
                lThreadCtx.start(lToken)
    def unregisterClass(self, pClassName, pHandler):
        def _unregci(_pServerToken):
            if self.mMvstoreInproc: mvstoreinproc.unregnotif(self.mDbConnection._s(None), _pServerToken, self.mClientID); return
            self._callServer("http://%s/db/?i=unregnotif&notifparam=%s" % (self.mDbConnection.host(), _pServerToken))
        lCtx = None
        with self.mLock:
            if not self.mClassNames.has_key(pClassName):
                return
            for iCtx in xrange(len(self.mClassNames[pClassName])):
                lCtx = self.mClassNames[pClassName][iCtx]
                if lCtx.mHandler == pHandler:
                    del self.mClassNames[pClassName][iCtx]
                    del self.mTokens[lCtx.mServerToken]
                    break
        if lCtx:
            _unregci(lCtx.mServerToken)
            lCtx.finished = True
    def registerPIN(self, pLocalPID, pHandler, pHandlerData=None, pGroupNotifs=True):
        "Registration of notifications for PIN-related changes."
        def _regpi(_pCriterion):
            if self.mMvstoreInproc: return mvstoreinproc.regnotif(self.mDbConnection._s(None), None, _pCriterion, self.mClientID)
            return self._callServer("http://%s/db/?i=regnotif&notifparam=%s&type=pin&clientid=%s" % (self.mDbConnection.host(), _pCriterion, self.mClientID))
        lCriterion = MVNotifier.serializeLocalPID(pLocalPID)
        lThreadCtx = MVNotifier._ThreadCtx(lCriterion, pHandler, pHandlerData, not pGroupNotifs, self)
        lResult = json.loads(_regpi(lCriterion))
        if lResult and len(lResult.keys()) > 0:
            with self.mLock:
                lToken = lResult.keys()[0]
                if not self.mPIDs.has_key(lCriterion):
                    self.mPIDs[lCriterion] = []
                self.mPIDs[lCriterion].append(lThreadCtx)
                self.mTokens[lToken] = lThreadCtx
                lThreadCtx.start(lToken)
    def unregisterPIN(self, pLocalPID, pHandler):
        def _unregpi(_pServerToken):
            if self.mMvstoreInproc: mvstoreinproc.unregnotif(self.mDbConnection._s(None), _pServerToken, self.mClientID); return
            self._callServer("http://%s/db/?i=unregnotif&notifparam=%s" % (self.mDbConnection.host(), _pServerToken))
        lCtx = None
        with self.mLock:
            lCriterion = MVNotifier.serializeLocalPID(pLocalPID)
            if not self.mPIDs.has_key(lCriterion):
                return
            for iCtx in xrange(len(self.mPIDs[lCriterion])):
                lCtx = self.mPIDs[lCriterion][iCtx]
                if lCtx.mHandler == pHandler:
                    del self.mPIDs[lCriterion][iCtx]
                    del self.mTokens[lCtx.mServerToken]
                    break
        if lCtx:
            _unregpi(lCtx.mServerToken)
            lCtx.finished = True
    def _callServer(self, pURL, pTimeout=None):
        assert not self.mMvstoreInproc
        lT1 = time.time()
        logging.debug(pURL)
        try:
            lResult = urllib2.urlopen(urllib2.Request(pURL, headers={"Authorization":"Basic %s" % self.mDbConnection.basicauth()}), timeout=pTimeout).read()
            logging.debug("received response (%ss)...\n  url: %s\n  response: %s\n" % (time.time() - lT1, pURL, lResult))
            return lResult
        except socket.timeout as ex:
            logging.info(repr(ex))
            return None
        except Exception as ex:
            logging.error(repr(ex))
            return None
    def _addPendingCtx(self, pThreadCtx):
        with self.mLock:
            self.mPendingCtxs.add(pThreadCtx)
    def _removePendingCtx(self, pThreadCtx):
        with self.mLock:
            self.mPendingCtxs.discard(pThreadCtx)
    def _entryPoint(self, pThreadCtx):
        def _waiti(_pArg):
            if self.mMvstoreInproc: return mvstoreinproc.waitnotif(self.mDbConnection._s(None), _pArg, 5000)
            return self._callServer("http://%s/db/?i=waitnotif&%s&timeout=5000" % (self.mDbConnection.host(), _pArg), pTimeout=10)
        while not pThreadCtx.finished:
            self._addPendingCtx(pThreadCtx)
            if pThreadCtx.mServerToken:
                lUrlArg = "notifparam=%s" % pThreadCtx.mServerToken
                lRawRet = _waiti(lUrlArg)
                if lRawRet != None:
                    lRet = json.loads(lRawRet)
                    lRetKeys = lRet.keys()
                    if len(lRetKeys) > 0 and lRetKeys[0] != "timeout":
                        if lRetKeys[0] != pThreadCtx.mServerToken:
                            logging.warn("unexpected response for token %s: %s" % (pThreadCtx.mServerToken, lRetKeys[0]))
                        pThreadCtx.mHandler(pThreadCtx.mHandlerData, lRet[lRetKeys[0]])
            else:
                lUrlArg = "clientid=%s" % pThreadCtx.mCriterion
                lRawRet = _waiti(lUrlArg)
                if lRawRet != None:
                    lRet = json.loads(lRawRet)
                    lRetKeys = lRet.keys()
                    if len(lRetKeys) > 0 and lRetKeys[0] != "timeout":
                        pThreadCtx.mHandler(pThreadCtx.mHandlerData, lRet)
            self._removePendingCtx(pThreadCtx)
    @staticmethod
    def _staticEntryPoint(*args, **kwargs):
        "The thread entry point for all _ThreadCtx.mThread objects."
        args[0]._entryPoint(args[1])
    @staticmethod
    def _groupHandler(pSelf, pNotifData):
        "The notification handler for notifications that are grouped by clientid; allows to use a single connection per client process for all notifications."
        lHandlers = []
        logging.debug("got group notif data: %s" % pNotifData)
        with pSelf.mLock:
            for iT in pNotifData.items():
                logging.debug("token %s: %s notifications" % (iT[0], len(iT[1])))
                try:
                    if pSelf.mTokens.has_key(iT[0]) and None == pSelf.mTokens[iT[0]].mThread:
                        lHandlers.append([pSelf.mTokens[iT[0]].mHandler, pSelf.mTokens[iT[0]].mHandlerData, iT[1]])
                except Exception as ex:
                    logging.warn("caught exception during notification: %s" % repr(ex))
        for iH in lHandlers:
            logging.debug("forwarded group notification for %s" % iH[2])
            iH[0](iH[1], iH[2])
    @staticmethod
    def serializeLocalPID(pLocalPID):
        "Normalized representation of PIDs for notification purposes."
        if isInteger(pLocalPID):
            return "%x" % pLocalPID
        return pLocalPID    
MVNOTIFIER = MVNotifier()
