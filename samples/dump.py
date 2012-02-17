#!/usr/bin/env python2.6
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
