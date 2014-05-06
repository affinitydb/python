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
"""Base module to orchestrate the automatic execution and validation of all tests."""

import datetime
import os
import re
import sys

AFFINITY_ALL_TESTS = {}

STRFTIME_IS_LIMITED = (datetime.datetime.utcnow().strftime("%4Y") == "4Y")
STRFTIME_RE_SIMPLIFY = re.compile(r'[0-9]*')

class AffinityTest(object):
    "Base class for all test scripts in this test fwk."
    @staticmethod
    def declare(pTestClass):
        "Service to register a test in the fwk."
        if sys.modules['__main__'].__dict__.has_key('AFFINITY_ALL_TESTS'):
            lTheTests = sys.modules['__main__'].__dict__['AFFINITY_ALL_TESTS']
            lTheTests[pTestClass.__name__.lower()] = pTestClass
    def getTags(self):
        "Returns an array of tags to classify the test."
        return ("unclassified")
    def execute(self):
        "This is the test's primary entry point, invoked by the fwk."
        return True
    @staticmethod
    def strftime(pDT, pFormat):
        if STRFTIME_IS_LIMITED:
            return pDT.strftime(STRFTIME_RE_SIMPLIFY.sub("", pFormat))
        return pDT.strftime(pFormat)

if __name__ == '__main__':
    def _onWalk(_pArg, _pDir, _pFileNames):
        "Traverse a directory structure and call _pArg[1] on every file matching extension _pArg[0]; stop after _pArg[3] > _pArg[2], unless they're null."
        for _f in _pFileNames:
            if (len(_pArg) > 3) and (_pArg[3] != None) and _pArg[2] and (_pArg[3] > _pArg[2]):
                return
            elif _f.rfind(".%s" % _pArg[0]) == len(_f) - 1 - len(_pArg[0]):
                if (len(_pArg) > 3) and (_pArg[3] != None):
                    _pArg[3] = _pArg[3] + 1
                _pArg[1](_pDir, _f)
            elif os.path.isdir(_f):
                os.path.walk(_f, _onWalk, _pArg)
    def _loadTest(_pDir, _pFN):
        "Load one python file, to give it a chance to register tests in AFFINITY_ALL_TESTS."
        _lN = _pFN.split(".")[0]
        if 0 == _lN.find("test") and _lN != "testfwk":
            try:
                __import__(_lN)
            except Exception as ex:
                pass
    def _loadAllTests():
        "Load all python files under the current directory, and give them a chance to register tests in AFFINITY_ALL_TESTS."
        _lArgs = ["py", _loadTest, None, None]
        os.path.walk(".", _onWalk, _lArgs)
    def _createTestInstance(_pTestName):
        "Create an instance of the test class whose name is _pTestName, if any."
        if AFFINITY_ALL_TESTS.has_key(_pTestName.lower()):
            _lTestClass = AFFINITY_ALL_TESTS[_pTestName.lower()]
            _lTest = _lTestClass.__new__(_lTestClass)
            if isinstance(_lTest, _lTestClass):
                type(_lTest).__init__(_lTest)
                return _lTest
            else:
                print ("WARNING: Inconsistent type for test %s" % _pTestName)
        else:
            print ("WARNING: Couldn't find test %s" % _pTestName)
        return None
    def _run(_pTestName):
        "Run <testname>."
        _lTest = _createTestInstance(_pTestName)
        if _lTest:
            _lTest.execute()
    def _runall(_pBogus):
        "Run all tests."
        print "Start to run all tests ... "
        _lTestN = AFFINITY_ALL_TESTS.keys()
        results = {} # casename and result mappings.
        for _i, _iT in zip(xrange(len(_lTestN)), _lTestN):
            print ("\nRunning test %s [%d/%d]\n" % (_iT, (1 + _i), len(_lTestN)))
            try:
                _run(_iT)
                results[_iT] = "Pass"
            except AssertionError, ex:
                print "AssertionError : " + repr(ex)
                results[_iT] = "Fail"

        print "All tests completed! "
        print "Python test results:"
        for each in results:
            print each + ":" +  results[each]

    _loadAllTests()
    lCmds = ("run", "runall")
    lCmdsi = (_run, _runall)
    lCmd = None
    lTestName = None
    if len(sys.argv) > 1:
        lCmd = sys.argv[1]
    if len(sys.argv) > 2:
        lTestName = sys.argv[2]
    if lCmd in lCmds:
        lCmdsi[lCmds.index(lCmd)](lTestName)
    else:        
        print ("Invalid command (%s) or test name (%s)" % (lCmd, lTestName))
        print ("\nHelp:")
        print ("  python testfwk.py <command> <testname>")
        print ("\nCommands:")
        for iC, iCi in zip(lCmds, lCmdsi):
            print ("  %s:\n    %s" % (iC, iCi.__doc__))
        print ("\nTest names:")
        for iT in AFFINITY_ALL_TESTS.iteritems():
            print ("  %s:\n    %s" % (iT[0], iT[1].__doc__))
