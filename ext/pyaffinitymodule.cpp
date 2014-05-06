/*
Copyright (c) 2004-2014 GoPivotal, Inc. All Rights Reserved.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,  WITHOUT
 WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 License for the specific language governing permissions and limitations
 under the License.
*/

// This is an in-proc alternative to the server.
// It provides an equivalent interface.

#include <Python.h>
#include <affinity.h>
#include <startup.h>
#include <vector>
#include <algorithm>
#ifndef WIN32
#define POSIX
#endif
#include "storenotifier.h"
#if 0
    #include "serialization.h"
#endif
using namespace Afy;

typedef IAffinity* AfyCtx;
AfyCtx gAfyCtx;
MainNotificationHandler gNotifHandler;
typedef std::vector<ISession *> TSessions;
TSessions gAfySessions; // Review: At some point will need locking also.
typedef std::vector<unsigned char> TStreamBuf;

class PyAfyResultStream : public IStreamIn
{
    public:
        PyAfyResultStream() : mIncoming(NULL) {}
        virtual ~PyAfyResultStream() { if (mIncoming) { mIncoming->destroy(); mIncoming = NULL; } }
        virtual RC next(const unsigned char *buf,size_t lBuf)
        {
            // Review: perf...
            for (size_t i = 0; i < lBuf; i++)
                mResult.push_back(buf[i]);
            return RC_OK;
        }
        virtual void destroy() {}
    public:
        void getResult(TStreamBuf & pResult) { pResult = mResult; mResult.clear(); }
        void setIncoming(IStreamIn * pIncoming) { mIncoming = pIncoming; }
        IStreamIn * getIncoming() const { return mIncoming; }
    protected:
        TStreamBuf mResult;
        IStreamIn * mIncoming;
};

static PyObject * affinityinproc_open(PyObject *self, PyObject *args)
{
    if (gAfyCtx)
        { PyErr_SetString(PyExc_RuntimeError, "Affinity already open!"); return NULL; }
    StartupParameters lParams;
    lParams.notification = &gNotifHandler;
    lParams.nBuffers = 10000; // TODO: configuration...
    RC lRC = openStore(lParams, gAfyCtx);
    if (RC_NOTFOUND == lRC)
    {
        StoreCreationParameters lCreate;
        lRC = createStore(lCreate, lParams, gAfyCtx);
    }
    return Py_BuildValue("i", lRC);
}

static PyObject * affinityinproc_startSession(PyObject *self, PyObject *args)
{
    // Note (maxw):
    //   At least for the time being, we consider here that it's
    //   the responsibility of the caller to make sure that only
    //   one session is assigned to a thread at any given point
    //   (using attachSession and detachSession).  IOW, this
    //   interface stands at the same logical level as in Affinity.
    if (!gAfyCtx)
        { PyErr_SetString(PyExc_RuntimeError, "No Affinity open!"); return NULL; }
    ISession * lSession = gAfyCtx->startSession();
    if (lSession)
    {
        gAfySessions.push_back(lSession);
        char lKey[32];
        sprintf(lKey, "%p", lSession);
        return Py_BuildValue("s", lKey);
    }
    PyErr_SetString(PyExc_RuntimeError, "Failed to startSession!");
    return NULL;
}

#define CHECK_SESSION2(pSk) \
    ISession * lSession; \
    if (!gAfyCtx) \
        { PyErr_SetString(PyExc_RuntimeError, "No Affinity open!"); return NULL; } \
    if (1 != sscanf(pSk, "%p", &lSession) || NULL == lSession) \
        { PyErr_SetString(PyExc_TypeError, "Invalid session!"); return NULL; }
#define CHECK_SESSION(pArgs) \
    char * lSessionKey; \
    if (!PyArg_ParseTuple(pArgs, "s", &lSessionKey)) \
        { PyErr_SetString(PyExc_TypeError, "Expected string session argument!"); return NULL; } \
    CHECK_SESSION2(lSessionKey)

static PyObject * affinityinproc_terminateSession(PyObject *self, PyObject *args)
{
    CHECK_SESSION(args);
    TSessions::iterator i = std::find(gAfySessions.begin(), gAfySessions.end(), lSession);
    if (gAfySessions.end() == i)
        { PyErr_SetString(PyExc_TypeError, "Invalid session!"); return NULL; }
    (*i)->terminate();
    gAfySessions.erase(i);
    return Py_BuildValue("i", 0);
}

static PyObject * affinityinproc_attachSession(PyObject *self, PyObject *args)
{
    CHECK_SESSION(args);
    return Py_BuildValue("i", lSession->attachToCurrentThread());
}

static PyObject * affinityinproc_detachSession(PyObject *self, PyObject *args)
{
    CHECK_SESSION(args);
    return Py_BuildValue("i", lSession->detachFromCurrentThread());
}

static PyObject * affinityinproc_post(PyObject *self, PyObject *args)
{
    // Note:
    //   We also offer an alternative: beginlongpost, continuelongpost, endlongpost.
    //   This is to enable synchronous responses inside a transaction.
    char * lSessionKey;
    char * lInputBuffer;
    int lInputBufferLen;
    if (!PyArg_ParseTuple(args, "st#", &lSessionKey, &lInputBuffer, &lInputBufferLen))
        { PyErr_SetString(PyExc_TypeError, "Expected 'st#' arguments!"); return NULL; }
    CHECK_SESSION2(lSessionKey);
    IStreamIn * lIn = NULL;
    #if 0 // For debugging...
        printf("*** about to process input stream...\n"); getchar();
    #endif
    PyAfyResultStream lOutput;
    if (RC_OK != lSession->createInputStream(lIn, &lOutput, 1024))
        { PyErr_SetString(PyExc_RuntimeError, "Affinity failed to createInputStream!"); return NULL; }
    RC lRC = lIn->next((unsigned char const *)lInputBuffer, lInputBufferLen);
    if (RC_OK == lRC)
        lRC = lIn->next(NULL, 0); // Final flush.
    lIn->destroy();
    TStreamBuf lResult; lOutput.getResult(lResult);
    return Py_BuildValue("(i,s#)", lRC, lResult.empty() ? NULL : &lResult[0], lResult.size());
}

static PyObject * affinityinproc_beginlongpost(PyObject *self, PyObject *args)
{
    char * lSessionKey;
    if (!PyArg_ParseTuple(args, "s", &lSessionKey))
        { PyErr_SetString(PyExc_TypeError, "Expected 's' arguments!"); return NULL; }
    CHECK_SESSION2(lSessionKey);
    IStreamIn * lIn = NULL;
    PyAfyResultStream * lOutput = new PyAfyResultStream();
    if (RC_OK != lSession->createInputStream(lIn, lOutput, 1024))
        { PyErr_SetString(PyExc_RuntimeError, "Affinity failed to createInputStream!"); return NULL; }
    lOutput->setIncoming(lIn);
    char lKey[32];
    sprintf(lKey, "%p", lOutput);
    return Py_BuildValue("s", lKey);
}

static PyObject * affinityinproc_continuelongpost(PyObject *self, PyObject *args)
{
    char * lSessionKey, * lLongPostKey;
    char * lInputBuffer;
    int lInputBufferLen;
    if (!PyArg_ParseTuple(args, "sst#", &lSessionKey, &lLongPostKey, &lInputBuffer, &lInputBufferLen))
        { PyErr_SetString(PyExc_TypeError, "Expected 'sst#' arguments!"); return NULL; }
    CHECK_SESSION2(lSessionKey);
    PyAfyResultStream * lOutput = NULL;
    if (1 != sscanf(lLongPostKey, "%p", &lOutput) || NULL == lOutput || NULL == lOutput->getIncoming())
        { PyErr_SetString(PyExc_TypeError, "Invalid longpost!"); return NULL; }
    RC lRC = lOutput->getIncoming()->next((unsigned char const *)lInputBuffer, lInputBufferLen);
    TStreamBuf lResult; lOutput->getResult(lResult);
    return Py_BuildValue("(i,s#)", lRC, lResult.empty() ? NULL : &lResult[0], lResult.size());
}

static PyObject * affinityinproc_endlongpost(PyObject *self, PyObject *args)
{
    char * lSessionKey, * lLongPostKey;
    if (!PyArg_ParseTuple(args, "ss", &lSessionKey, &lLongPostKey))
        { PyErr_SetString(PyExc_TypeError, "Expected 'ss' arguments!"); return NULL; }
    CHECK_SESSION2(lSessionKey);
    PyAfyResultStream * lOutput = NULL;
    if (1 != sscanf(lLongPostKey, "%p", &lOutput) || NULL == lOutput || NULL == lOutput->getIncoming())
        { PyErr_SetString(PyExc_TypeError, "Invalid longpost!"); return NULL; }
    RC lRC = lOutput->getIncoming()->next(NULL, 0); // Final flush.
    TStreamBuf lResult; lOutput->getResult(lResult);
    delete lOutput;
    return Py_BuildValue("(i,s#)", lRC, lResult.empty() ? NULL : &lResult[0], lResult.size());
}

static PyObject * affinityinproc_get(PyObject *self, PyObject *args)
{
    char * lSessionKey;
    char * lPathsqlCmd;
    PyObject * lOptions = NULL;
    if (!PyArg_ParseTuple(args, "ss|O", &lSessionKey, &lPathsqlCmd, &lOptions))
        { PyErr_SetString(PyExc_TypeError, "Expected 'ss|O' arguments!"); return NULL; }
    CHECK_SESSION2(lSessionKey);
    CompilationError lCE;
    IStmt * const lStmt = lSession->createStmt(lPathsqlCmd, NULL, 0, &lCE);
    if (!lStmt)
    {
        PyErr_Format(PyExc_RuntimeError, "pathSQL syntax error [%s]: %s at %d, line %d\n", lPathsqlCmd, lCE.msg, lCE.pos, lCE.line);
        return NULL;
    }
    PyObject * lRet = NULL;
    if (NULL != lOptions && NULL != PyDict_GetItemString(lOptions, "count")) // Count
    {
        uint64_t lCount;
        RC const lRC = lStmt->count(lCount);
        if (RC_OK != lRC)
        {
            PyErr_Format(PyExc_RuntimeError, "Affinity error: %d", lRC);
            return NULL;
        }
        lRet = Py_BuildValue("i", lCount);
    }
    else
    {
        unsigned long const lLimit = (lOptions && PyDict_GetItemString(lOptions, "limit")) ? PyLong_AsLong(PyDict_GetItemString(lOptions, "limit")) : ~0u;
        unsigned long const lOffset = (lOptions && PyDict_GetItemString(lOptions, "offset")) ? PyLong_AsLong(PyDict_GetItemString(lOptions, "offset")) : 0;
        IStreamOut * lOut = NULL;
        RC const lRC = lStmt->execute(lOut, NULL, 0, lLimit, lOffset);
        TStreamBuf lResult;
        if (RC_OK != lRC)
        {
            PyErr_Format(PyExc_RuntimeError, "Affinity error: %d", lRC);
            return NULL;
        }
        else
        {
            unsigned char lBuf[0x1000];
            size_t lRead = 0x1000;
            RC lRC;
            while (RC_OK == (lRC = lOut->next(lBuf, lRead)))
            {
                // Review: perf...
                for (size_t i = 0; i < lRead; i++)
                    lResult.push_back(lBuf[i]);
                lRead = 0x1000;
            }
            if (RC_OK != lRC && RC_EOF != lRC)
                printf("Affinity error: %d\n", lRC);
            lOut->destroy();
        }
        lRet = Py_BuildValue("s#", (0 == lResult.size()) ? NULL : &lResult[0], lResult.size());
    }
    lStmt->destroy();
    return lRet;
}

static PyObject * affinityinproc_check(PyObject *self, PyObject *args)
{
    char * lSessionKey;
    char * lPathsqlCmd;
    PyObject * lOptions = NULL;
    if (!PyArg_ParseTuple(args, "ss|O", &lSessionKey, &lPathsqlCmd, &lOptions))
        { PyErr_SetString(PyExc_TypeError, "Expected 'ss|O' arguments!"); return NULL; }
    CHECK_SESSION2(lSessionKey);
    #if 0
    {
        IStmt * lS = lSession->createStmt();
        lS->addVariable();
        ICursor * lRes = lS->execQ();
        IPIN * lPIN;
        while (lRes && NULL != (lPIN = lRes->next()))
        {
            MvStoreSerialization::ContextOutDbg lSerCtx(std::cout, lSession, 64, MvStoreSerialization::ContextOutDbg::kFShowPropIds);
            MvStoreSerialization::OutDbg::pin(lSerCtx, *lPIN);
            lPIN->destroy();
        }
        lRes->destroy();
        lS->destroy();
    }
    #endif
    CompilationError lCE;
    char * lRawResult = NULL;
    unsigned long const lLimit = (lOptions && PyDict_GetItemString(lOptions, "limit")) ? PyLong_AsLong(PyDict_GetItemString(lOptions, "limit")) : ~0u;
    unsigned long const lOffset = (lOptions && PyDict_GetItemString(lOptions, "offset")) ? PyLong_AsLong(PyDict_GetItemString(lOptions, "offset")) : 0;
    const RC lRC = lSession->execute(lPathsqlCmd, strlen(lPathsqlCmd), &lRawResult, NULL, 0, NULL, 0, &lCE, NULL, lLimit, lOffset);
    if (RC_SYNTAX == lRC && lCE.msg)
        printf("%*s\nSyntax: %s at %d, line %d\n", lCE.pos+2, "^", lCE.msg, lCE.pos, lCE.line);
    else if (RC_OK != lRC)
        printf("Affinity error: %d\n", lRC);
    PyObject * const lResult = Py_BuildValue("s", lRawResult);
    if (lRawResult)
        lSession->free(lRawResult);
    return lResult;
}

static PyObject * affinityinproc_close(PyObject *self, PyObject *args)
{
    if (!gAfyCtx)
        { PyErr_SetString(PyExc_RuntimeError, "Affinity not open!"); return NULL; }
    assert(gAfySessions.empty());
    gAfyCtx->shutdown(); gAfyCtx = NULL;
    return Py_BuildValue("i", 0);
}

static PyObject * affinityinproc_regnotif(PyObject *self, PyObject *args)
{
    char * lSessionKey;
    char * lClassName;
    char * lPIDstr;
    char * lClientID;
    if (!PyArg_ParseTuple(args, "szzs", &lSessionKey, &lClassName, &lPIDstr, &lClientID))
        { PyErr_SetString(PyExc_TypeError, "Expected 'szzs' arguments!"); return NULL; }
    CHECK_SESSION2(lSessionKey);
    char * lRawResult = NULL;
    RC const lRC = afy_regNotifi(gNotifHandler, *lSession, lClassName ? "class" : "pin", lClassName ? lClassName : lPIDstr, lClientID, &lRawResult);
    if (RC_OK != lRC || !lRawResult)
        { PyErr_SetString(PyExc_TypeError, "Failed to register notification handler"); return NULL; }
    PyObject * const lResult = Py_BuildValue("s", lRawResult);
    lSession->free(lRawResult);
    return lResult;
}

static PyObject * affinityinproc_unregnotif(PyObject *self, PyObject *args)
{
    char * lSessionKey;
    char * lServerToken;
    char * lClientID;
    if (!PyArg_ParseTuple(args, "sss", &lSessionKey, &lServerToken, &lClientID))
        { PyErr_SetString(PyExc_TypeError, "Expected 'sss' arguments!"); return NULL; }
    CHECK_SESSION2(lSessionKey);
    char * lRawResult = NULL;
    RC const lRC = afy_unregNotifi(gNotifHandler, *lSession, lSessionKey, lClientID, &lRawResult);
    if (RC_OK != lRC || !lRawResult)
        { PyErr_SetString(PyExc_TypeError, "Failed to unregister notification handler"); return NULL; }
    PyObject * const lResult = Py_BuildValue("s", lRawResult);
    lSession->free(lRawResult);
    return lResult;
}

static PyObject * affinityinproc_waitnotif(PyObject *self, PyObject *args)
{
    char * lSessionKey;
    char * lArg;
    int lTimeout = 0;
    if (!PyArg_ParseTuple(args, "ssi", &lSessionKey, &lArg, &lTimeout))
        { PyErr_SetString(PyExc_TypeError, "Expected 'ssi' arguments!"); return NULL; }
    CHECK_SESSION2(lSessionKey);
    char * lNotifParam = NULL;
    char * lClientID = NULL;
    static char const * const sNP = "notifparam=";
    static char const * const sCI = "clientid=";
    if (lArg == strstr(lArg, sNP) && strlen(lArg) > strlen(sNP))
        { lNotifParam = &lArg[strlen(sNP)]; }
    else if (lArg == strstr(lArg, sCI) && strlen(lArg) > strlen(sCI))
        { lClientID = &lArg[strlen(sCI)]; }
    char * lRawResult = NULL;
    RC lRC;
    Py_BEGIN_ALLOW_THREADS
    lRC = afy_waitNotifi(gNotifHandler, *lSession, lNotifParam, lClientID, lTimeout, &lRawResult);
    Py_END_ALLOW_THREADS
    if (RC_OK != lRC || !lRawResult)
        { PyErr_SetString(PyExc_TypeError, "Failed to wait for notification"); return NULL; }
    PyObject * const lResult = Py_BuildValue("s", lRawResult);
    lSession->free(lRawResult);
    return lResult;
}

static PyMethodDef affinityinprocMethods[] =
{
    {"open", affinityinproc_open, METH_VARARGS, "Open the store."},
    {"startSession", affinityinproc_startSession, METH_VARARGS, "Start a store session."},
    {"terminateSession", affinityinproc_terminateSession, METH_VARARGS, "Terminate the specified store session."},
    {"attachSession", affinityinproc_attachSession, METH_VARARGS, "Attach the specified store session to the current thread."},
    {"detachSession", affinityinproc_detachSession, METH_VARARGS, "Detach the specified store session from the current thread."},
    {"post", affinityinproc_post, METH_VARARGS, "Execute a protobuf stream."},
    {"beginlongpost", affinityinproc_beginlongpost, METH_VARARGS, "Begin executing a 'long' protobuf stream (enabling intermittent responses)."},
    {"continuelongpost", affinityinproc_continuelongpost, METH_VARARGS, "Continue executing a 'long' protobuf stream."},
    {"endlongpost", affinityinproc_endlongpost, METH_VARARGS, "End executing a 'long' protobuf stream."},
    {"get", affinityinproc_get, METH_VARARGS, "Execute a pathSQL statement and return a protobuf stream."},
    {"check", affinityinproc_check, METH_VARARGS, "Execute a pathSQL statement and return a string representation (pseudo-json)."},
    {"close", affinityinproc_close, METH_VARARGS, "Close the store."},
    {"regnotif", affinityinproc_regnotif, METH_VARARGS, "Register a handler for specific notifications."},
    {"unregnotif", affinityinproc_unregnotif, METH_VARARGS, "Unregister a notification handler."},
    {"waitnotif", affinityinproc_waitnotif, METH_VARARGS, "Wait for notifications."},
    {NULL, NULL, 0, NULL}
};

PyMODINIT_FUNC initaffinityinproc(void)
{
    (void) Py_InitModule("affinityinproc", affinityinprocMethods);
}

int main(int argc, char *argv[])
{
    Py_SetProgramName(argv[0]);
    Py_Initialize();
    initaffinityinproc();
}
