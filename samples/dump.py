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
"""This module performs a simple store dump, and demonstrates basic querying options."""
from affinity import *
import time

if __name__ == '__main__':
    lAffinity = AFFINITY()
    lAffinity.open()
    # Define the parameters of the query.
    lQuery = "SELECT *"
    lCount = lAffinity.qCount(lQuery)
    print ("TOTAL COUNT: %s" % lCount)
    time.sleep(1)
    lPageSize = 200
    lProtoOut = True
    # Go.
    lOffset = 0
    while lOffset < lCount:
        lOptions = {"limit":lPageSize, "offset":lOffset}
        if lProtoOut: # Protobuf output.
            lPins = PIN.loadPINs(lAffinity.qProto(lQuery, lOptions))
            for iP in lPins:
                print (iP)
        else: # JSON output.
            print (lAffinity.check(lQuery, lOptions))
        lOffset = lOffset + lPageSize
    lAffinity.close()
