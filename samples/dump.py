#!/usr/bin/env python2.6
"""This module performs a simple dump."""
from mvstore import *

if __name__ == '__main__':
    lMvStore = MVSTORE()
    lMvStore.open()
    lPins = PIN.loadPINs(lMvStore.mvsqlProto("SELECT *;"))
    for iP in lPins:
        print iP
    lMvStore.close()
