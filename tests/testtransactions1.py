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

# TODO: test in-place modifs more in depth (with tx etc.)
# TODO: test MODE_IMMEDIATE_UPDATES vs not
# TODO: make sure that changes across pins, and multi-changes on a property, work well

from copy import copy
from affinity import *
import random
import time

if __name__ == '__main__':
    lAffinity = AFFINITY()
    lAffinity.open()

    # start by exploring the python-specific aspects (until locking is reenabled, it's difficult to assess transactions)
    # ---
    # create 2 threads
    # have a step value
    # each thread waits for the step, and performs

    lAffinity.close()
