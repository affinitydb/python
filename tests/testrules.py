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

"""This test case is to test Affinity's rules."""

from testfwk import AffinityTest
from affinity import *
from utils import *
import random
import os
import time


# A basic test case for testing rules:
# create a temperature condition and a distance condition;
# create an action to display a message.
# create a rule to combine the conditions and the action.
# a lot of transient pins are created, some of them may satisfy the conditions(temperature>60 and distance<1),
# then the action will be performed, the WARNING message is displayed.
def test_case_1():
    
    lAffinity = AFFINITY()
    lAffinity.open()
    # create IO service pin to display the warning message
    lAffinity.q("INSERT afy:objectID='display_fire_warning',afy:service=.srv:IO,afy:address=1;")

    lAffinity.q("INSERT afy:objectID='count_fire_warning',fire_warning_cnt=0;")

    # create a temperature condition
    lAffinity.q("CREATE CONDITION temperature_warning AS temperature>:0")

    # create a distance condition
    lAffinity.q("CREATE CONDITION distance_warning AS distance<:0;")

    # create an action to display the warning message
    lAffinity.q("CREATE ACTION action_fire_warning AS UPDATE #display_fire_warning SET afy:content='FIRE WARNING!\n';")

    lAffinity.q("CREATE ACTION action_update_count_warning AS UPDATE #count_fire_warning SET fire_warning_cnt+=1;")
    
    # create a rule
    lAffinity.q("RULE fire_warning : temperature_warning(60) AND distance_warning(1) -> action_fire_warning,action_update_count_warning;")
    
    cnt=0
    for i in range(10000):
        temperature = random.uniform(-50, 100)
        distance = random.uniform(0, 100)
        if temperature>60 and distance<1:
            cnt=cnt+1
        sql = "INSERT OPTIONS(TRANSIENT) temperature="+ str(temperature) +",distance=" + str(distance)
        lAffinity.q(sql)

    lPins = PIN.loadPINs(lAffinity.qProto("SELECT * FROM #count_fire_warning"))

    print "==> Totally " + str(cnt) + " fire warnings are detected!"
    assert lPins[0]['fire_warning_cnt'] == cnt
    
    lAffinity.close()
    return True 

def _entryPoint():
    start = time.time()
    test_case_1()
    print "Test rules complete!"
    end = time.time()
    print "Testrules costs : " + timeToString(end - start)
    
class TestRules(AffinityTest):
    def execute(self):
        _entryPoint()

AffinityTest.declare(TestRules)

if __name__ == '__main__':
    lT = TestRules()
    lT.execute()
    

