#!/usr/bin/env python2.6
# Copyright (c) 2004-2013 GoPivotal, Inc. All Rights Reserved.
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

"""This test case is to test Affinity's transient pins and other features, like triggers."""

from testfwk import AffinityTest
from affinity import *
from utils import *
import random
import os

# This case simulates handling data streams from a traffic sensor, the data source generates streams with such form (tid, speed, time)
# if the car's speed exceeds 200, it is considered as a violation of traffic rules and its status will be inserted 
# into store.
def test_case_1():
    TOTAL=1000
    lAffinity = AFFINITY()
    lAffinity.open()
    lAffinity.q("START TRANSACTION;");
    # define a class of VIOLATION the traffic rules
    lAffinity.q("CREATE CLASS VIOLATION AS (SELECT * WHERE EXISTS(TID) AND SPEED>200) SET afy:onEnter=${INSERT TID=@.TID}")
    # may cause segment fault with #1079
    # lAffinity.q("CREATE CLASS VIOLATION AS (SELECT * WHERE EXISTS(TID) AND SPEED>200) SET afy:onEnter=${INSERT TID=@.TID, SPEED=@.SPEED, TIME=@.TIME}")

    records={}
    for i in range(TOTAL):
        speed = random.randrange(50, 300)
        if(speed > 200):
            records[str(i)] = speed
        sql="INSERT OPTIONS(TRANSIENT) (TID, SPEED, TIME) VALUES (%d, %f, CURRENT_TIMESTAMP);" % (i, speed)
        lAffinity.q(sql)
    
    print lAffinity.qCount("SELECT * WHERE EXISTS(TID);")
    print lAffinity.qCount("SELECT * FROM VIOLATION;")
    assert lAffinity.qCount("SELECT * WHERE EXISTS(TID);") == len(records)
    assert lAffinity.qCount("SELECT * FROM VIOLATION;") == 0
    lAffinity.q("COMMIT;");
    lAffinity.close()
    print "Test test_case_1 complete!"
    return True

# This case simulates handling data streams from temperature sensors in a warehouse, 
# the data source generates streams with such form (fid, temperature, distance, time)
# if the temperature is higher than 50C, and distance is less than 5 meters, then it will be considered a warning 
# and inserted into store
def test_case_2():
    # for testing PERSIST statement 
    TOTAL=1000000
    lAffinity = AFFINITY()
    lAffinity.open()
    # define a class for fire warning
    lAffinity.q("CREATE CLASS FIRE_WARNING AS (SELECT * WHERE EXISTS(FID) AND TEMPERATURE> 50 AND DISTANCE < 5) SET afy:onEnter=${PERSIST}")
    
    records={}
    for i in range(TOTAL):
        temp = random.randrange(-20, 100)
        distance = random.randrange(0, 100)
        if(temp > 50 and distance < 5):
            records[str(i)] = temp
        sql="INSERT OPTIONS(TRANSIENT) (FID, TEMPERATURE, DISTANCE, TIME) VALUES (%d, %f, %f, CURRENT_TIMESTAMP);" % (i, temp, distance)
        lAffinity.q(sql)

    assert lAffinity.qCount("SELECT * WHERE EXISTS(FID);") == len(records)
    assert lAffinity.qCount("SELECT * FROM FIRE_WARNING;") == len(records)   
    lAffinity.close()
    print "Test test_case_2 complete!"
    return True

# This case simulates a ordering system. A lot of orders will be inserted, if the amount of one order(single price multipies count) 
# is larger than 10000 dollars, it is considered a "big deal", and a tigger is fired.
def test_case_3():
    # for testing the feature that TRANSIENT pins can be used to pass messages between classes
    TOTAL=10000
    lAffinity = AFFINITY()
    lAffinity.open()
    # define a class ORDERS 
    lAffinity.q("CREATE CLASS ORDERS AS SELECT * WHERE EXISTS(OID) AND EXISTS(PRICE) AND EXISTS(CNT) SET afy:onEnter=${INSERT OPTIONS(TRANSIENT) OID=@.OID, TOTAL_PRICE=((@.PRICE) * (@.CNT))}")
    
    # define a class BIG_DEAL
    lAffinity.q("CREATE CLASS BIG_DEAL AS SELECT * WHERE EXISTS(OID) AND TOTAL_PRICE > 10000 SET afy:onEnter=${PERSIST}")
    
    records={}
    for i in range(TOTAL):
        price = random.randrange(1, 1000)
        cnt = random.randrange(0, 100)
        if(price*cnt > 10000):
            records[str(i)] = price
        sql="INSERT (OID, PRICE, CNT, TIME) VALUES (%d, %f, %f, CURRENT_TIMESTAMP);" % (i, price, cnt)
        lAffinity.q(sql)
    
    print lAffinity.qCount("SELECT * FROM BIG_DEAL;")
    assert lAffinity.qCount("SELECT * FROM ORDERS;") == TOTAL
    assert lAffinity.qCount("SELECT * FROM BIG_DEAL;") == len(records)   
    lAffinity.close()
    print "Test test_case_3 complete!"
    return True    

def test_case_4():
    # for testing PERSIST ALL
    TOTAL=100000
    lAffinity = AFFINITY()
    lAffinity.open()
    lAffinity.q("CREATE CLASS DIVIDE_EXACTLY_BY_9 AS SELECT * WHERE PVALUE0%9 = 0 SET afy:onEnter=${INSERT OPTIONS(TRANSIENT) PVALUE1=@.PVALUE0}")
    lAffinity.q("CREATE CLASS DIVIDE_EXACTLY_BY_63 AS SELECT * WHERE PVALUE1%7 = 0 SET afy:onEnter=${INSERT OPTIONS(TRANSIENT) PVALUE2=@.PVALUE1}")
    # when the value can be divided exactly by 315, then triggered pins could be stored
    lAffinity.q("CREATE CLASS DIVIDE_EXACTLY_BY_315 AS SELECT * WHERE PVALUE2%5 = 0 SET afy:onEnter=${PERSIST ALL}")
    
    cnt=0
    for i in range(TOTAL):
        if(i%315 == 0):
            cnt+=1
        sql="INSERT OPTIONS(TRANSIENT) PVALUE0=%d;" % i
        lAffinity.q(sql)
    
    assert lAffinity.qCount("SELECT * WHERE EXISTS(PVALUE0);") == cnt
    assert lAffinity.qCount("SELECT * WHERE EXISTS(PVALUE1);") == cnt
    assert lAffinity.qCount("SELECT * WHERE EXISTS(PVALUE2);") == cnt
    assert lAffinity.qCount("SELECT * FROM DIVIDE_EXACTLY_BY_9") == cnt
    assert lAffinity.qCount("SELECT * FROM DIVIDE_EXACTLY_BY_63") == cnt
    assert lAffinity.qCount("SELECT * FROM DIVIDE_EXACTLY_BY_315") == cnt
    lAffinity.close()
    print "Test test_case_4 complete!"
    return True

def test_case_5():
    # for testing afy:onUpdate
    TOTAL=1000
    lAffinity = AFFINITY()
    lAffinity.open()
    
    lAffinity.q("CREATE CLASS T5_C1 AS SELECT * WHERE EXISTS(T5_P1) AND EXISTS(T5_P2) SET afy:onUpdate=${INSERT T5_UPD_P1=@.T5_P1, T5_UPD_P2=@.T5_P2};")
    
    lAffinity.q("CREATE CLASS T5_UPD_C1 AS SELECT * WHERE EXISTS(T5_UPD_P1) AND EXISTS(T5_UPD_P2)")
    
    # insert pins
    for i in range(TOTAL):
        sql="INSERT (T5_P1, T5_P2) VALUES (%d, '%s');" % (i,''.join(random.choice(string.letters) for i in xrange(random.randint(1,20))))
        lAffinity.q(sql)
    
    # update pins which may trigger afy:onUpdate
    cnt=0
    for i in range(TOTAL):
        if (random.randint(0,2) > 1) == True:
            sql="UPDATE T5_C1 SET T5_P2='%s' WHERE T5_P1=%i;" % (''.join(random.choice(string.letters) for i in xrange(random.randint(1,20))), i)
            lAffinity.q(sql)
            cnt=cnt+1
    
    # verify the results
    assert lAffinity.qCount("SELECT * FROM T5_C1;") == TOTAL
    assert lAffinity.qCount("SELECT * FROM T5_UPD_C1;") == cnt
    lAffinity.close()
    print "Test test_case_5 complete!"
    return True

def test_case_6():
    # for testing afy:onLeave
    TOTAL=1000
    lAffinity = AFFINITY()
    lAffinity.open()
    
    lAffinity.q("CREATE CLASS T6_C1 AS SELECT * WHERE EXISTS(T6_P1) AND EXISTS(T6_P2) SET afy:onLeave=${INSERT T6_LEV_P1=@.T6_P1, T6_LEV_TIME=CURRENT_TIMESTAMP};")
    lAffinity.q("CREATE CLASS T6_LEV_C1 AS SELECT * WHERE EXISTS(T6_LEV_P1);")

    # insert pins
    for i in range(TOTAL):
        sql="INSERT (T6_P1, T6_P2) VALUES (%d, '%s');" % (i,''.join(random.choice(string.letters) for i in xrange(random.randint(1,20))))
        lAffinity.q(sql)
    assert lAffinity.qCount("SELECT * FROM T6_C1;") == TOTAL

    # update pins which may trigger afy:onLeave
    cnt=0
    for i in range(TOTAL):
        if (random.randint(0,2) > 1) == True:
            sql="UPDATE T6_C1 DELETE T6_P2 WHERE T6_P1=%i;" %  i
            lAffinity.q(sql)
            cnt=cnt+1

    # verify the results
    assert lAffinity.qCount("select * where exists(T6_P2);") == (TOTAL - cnt)
    assert lAffinity.qCount("SELECT * FROM T6_LEV_C1;") == cnt
    lAffinity.close()
    print "Test test_case_6 complete!"
    return True

def _entryPoint():
    start = time.time()
    test_case_1()
    test_case_2()
    test_case_3()
    test_case_4()
    test_case_5()
    test_case_6()
    end = time.time()
    print "Test transient pins complete!"
    print "TestTransient costs : " + timeToString(end - start)
    
class TestTransient(AffinityTest):
    def execute(self):
        _entryPoint()

AffinityTest.declare(TestTransient)

if __name__ == '__main__':
    lT = TestTransient()
    lT.execute()

