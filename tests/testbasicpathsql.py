#!/usr/bintenv python2.6
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
"""This case is a regression testing for basic PathSQL statements. """

from testfwk import AffinityTest
from affinity import *
from utils import *
import random
import os

def test_case_1():
    # for testing PREFIX
    # There is no more SET BASE and PREFIX. Now SET PREFIX acts as a pseudo-statement,
    # It acts a bit like a #define. It's no longer memorized in a session-wide manner.  
    # It only acts on the current statement (or group of statements).  
    # The rationale is that when you insert program elements (timers, classes etc.), they're compiled straight away, 
    # so the kernel only needs to know about prefixes during that short period. 
    lAffinity = AFFINITY()
    lAffinity.open(pKeepAlive=True)

    lAffinity.q("insert prop1=10")
    assert(1 == lAffinity.qCount("select * where exists(prop1)"))
    lPins = PIN.loadPINs(lAffinity.qProto("select * where exists(prop1);"))
    assert(10 == lPins[0]['prop1'])
    
    lAffinity.q("insert prop2=11")
    assert(1 == lAffinity.qCount("select * where exists(prop2)"))
    lPins = PIN.loadPINs(lAffinity.qProto("select * where exists(prop2);"))
    assert(11 == lPins[0]['prop2'])
    
    lAffinity.setPrefix("customer", "http://foo.trading/customer")
    lAffinity.setPrefix("product", "http://foo.trading/product")
    lAffinity.setPrefix("orders", "http://foo.trading/orders")
    lAffinity.q("create class customer as select * where exists(customer:id) and exists(customer:name)")
    lAffinity.q("create class product as select * where exists(product:id) and exists(product:name)")
    lAffinity.q("create class orders as select * where exists(orders:id)")

    lAffinity.q("insert (customer:id, customer:name) values (1, \'Albert\')")
    lAffinity.q("insert (customer:id, customer:name) values (2, \'Black\')")
    lAffinity.q("insert (product:id, product:name) values (101, \'notebook\')")
    lAffinity.q("insert (product:id, product:name) values (102, \'ipad\')")
    lAffinity.q("insert (product:id, product:name) values (103, \'iphone\')")
    lAffinity.q("insert (orders:id, customer:id, orders:cnt) values (1001, 1, 3)")
    lAffinity.q("insert (orders:id, customer:id, orders:cnt) values (1002, 2, 17)")  
    
    assert (lAffinity.qCount("select * from customer") == 2)
    assert (lAffinity.qCount("select * from product") == 3)
    assert (lAffinity.qCount("select * from orders") == 2)

    # this statement caused an exception
    # lAffinity.qProto("select c.customer:name from customer as c join orders as o on(c.customer:id=o.customer:id) where (o.orders:cnt < 10);")
    assert (lAffinity.qCount("select c.customer:name from customer as c join orders as o on(c.customer:id=o.customer:id) where (o.orders:cnt < 10);") == 1)

    lAffinity.close()
    return True

def test_case_2():
    # for testing "PART" and repro bug#333
    lAffinity = AFFINITY()
    lAffinity.open()
    lAffinity.q("INSERT PROP_PARENT1=(INSERT PART PROP_CHILD1=2);")
    assert (lAffinity.qCount("SELECT * WHERE EXISTS(PROP_PARENT1) OR EXISTS(PROP_CHILD1);") == 2)
    # delete the parent pin --> delete child pin
    lAffinity.q("DELETE FROM * WHERE EXISTS(PROP_PARENT1);");
    assert (lAffinity.qCount("SELECT * WHERE EXISTS(PROP_PARENT1) OR EXISTS(PROP_CHILD1);") == 0)
    lAffinity.close()
    return True

def test_case_3():
    # for repro bug#329
    
    lAffinity = AFFINITY()
    lAffinity.open()
    lAffinity.q("INSERT a=1, b=1;")
    assert(lAffinity.qCount("select * where exists(a)") == 1)
    lPins = PIN.loadPINs(lAffinity.qProto("select * where exists(a)"))
    sql = "UPDATE " + str(lPins[0].mPID) + " SET a=a+1;"
    lAffinity.q(sql)
    assert(lAffinity.qCount("select * where a=2") == 1)
    sql = "UPDATE " + str(lPins[0].mPID) + " SET a=b;"
    lAffinity.q(sql)
    assert(lAffinity.qCount("select * where a=1 and b=1") == 1)

    sql = "UPDATE " + str(lPins[0].mPID) + " SET a=a+b;"
    lAffinity.q(sql)
    assert(lAffinity.qCount("select * where a=2 and b=1") == 1)
    lAffinity.q(sql)
    sql = "UPDATE " + str(lPins[0].mPID) + " SET a+=b;"
    assert(lAffinity.qCount("select * where a=3 and b=1") == 1)
    
    sql = "UPDATE " + str(lPins[0].mPID) + " SET a=a+" + str(lPins[0].mPID) + ".b"
    lAffinity.q(sql)
    assert(lAffinity.qCount("select * where a=4 and b=1") == 1)
    lAffinity.close()
    return True

def test_case_4():
    # for repro bug#332
    lAffinity = AFFINITY()
    lAffinity.open()    
    lAffinity.q("CREATE CLASS c AS SELECT * WHERE EXISTS(thing1)")
    lAffinity.q("CREATE CLASS c2 AS SELECT * WHERE angle=:0")
    lAffinity.q("INSERT test_case_4_a=1")
    lAffinity.q("INSERT test_case_4_a=2")
    pin1 = PIN.loadPINs(lAffinity.qProto("select * where test_case_4_a=1"))
    pin2 = PIN.loadPINs(lAffinity.qProto("select * where test_case_4_a=2"))
    sql = "UPDATE " + str(pin2[0].mPID) + " ADD thing1=" + str(pin1[0].mPID) + ", angle=10, ts=CURRENT_TIMESTAMP"
    lAffinity.q(sql)
    lAffinity.q(sql)
    assert(lAffinity.qCount("select * from c") == 1)
    assert(lAffinity.qCount("select * from c2(10)") == 1)
    lAffinity.close()
    return True

def test_case_5():
    # for testing nested INSERTS(inside arbitrary deep hierarchy of VT_STRUCT and VT_ARRAY)
    lAffinity = AFFINITY()
    lAffinity.open()    
    lAffinity.q("INSERT PROP1=(INSERT PROP1=(INSERT PROP1=(INSERT PROP1=(INSERT PROP1=(INSERT PROP1=13)))))")
    lAffinity.q("CREATE CLASS TEMP1 AS SELECT * WHERE EXISTS(PROP1)")
    assert(lAffinity.qCount("SELECT * from TEMP1") == 6)
    lAffinity.close()
    return True    

def test_case_6():
    # for reproducing bug#66, (concatenation of adjacent strings)
    lAffinity = AFFINITY()
    lAffinity.open()
    lAffinity.q("SELECT \'foobar\'")
    lAffinity.q("SELECT \'foo\' \n \'bar\'")
    lAffinity.q("INSERT BUG66_PROP=\'foobar\'")
    assert(lAffinity.qCount("SELECT * WHERE BUG66_PROP=\'foobar\'") == 1)
    assert(lAffinity.qCount("SELECT * WHERE BUG66_PROP=\'foo\' \n \'bar\'") == 1)
    lAffinity.close()
    return True
    
def test_case_7():
    # for reproducing bug#342
    lAffinity = AFFINITY()
    lAffinity.open()
    lAffinity.q("INSERT address_t7=\'1234 Sherbrooke\', machines={(INSERT type=\'alarm\', model=\'123\',state=0), (INSERT type=\'washer\', model=\'456\')};")
    lAffinity.q("CREATE CLASS homes AS SELECT * WHERE EXISTS(machines);")
    lAffinity.q("CREATE CLASS c7 AS SELECT * WHERE EXISTS(signal) SET machine=(SELECT * WHERE type=\'washer\'), afy:onEnter=${UPDATE @class ADD _home=(SELECT afy:pinID FROM homes WHERE @class.machine=machines)};")
    lAffinity.q("INSERT signal=1;")
    lAffinity.close()
    return True    

def test_case_8():
    # for reproducing bug#315
    lAffinity = AFFINITY()
    lAffinity.open()
    lAffinity.q("CREATE CLASS c315_2 as SELECT * WHERE exists(wt) AND COUNT(wt) >= 5 AND NOT EXISTS(et);")
    lAffinity.q("INSERT wt=1")
    lAffinity.q("INSERT et=1")
    pin = PIN.loadPINs(lAffinity.qProto("select * where wt=1"))
    sql = "UPDATE " + str(pin[0].mPID) + " ADD wt=2"
    lAffinity.q(sql)
    sql = "UPDATE " + str(pin[0].mPID) + " ADD wt=3"
    lAffinity.q(sql)
    sql = "UPDATE " + str(pin[0].mPID) + " ADD wt=4"
    lAffinity.q(sql)
    sql = "UPDATE " + str(pin[0].mPID) + " ADD wt=5 WHERE NOT EXISTS(patato)"
    lAffinity.q(sql)
    assert lAffinity.qCount("select * from c315_2;") == 1
    lAffinity.close()
    return True

def test_case_9():
    # for reproducing bug#278
    lAffinity = AFFINITY()
    lAffinity.open()
    lAffinity.q("INSERT \"a/b/c/d/e\"=3")
    pin = PIN.loadPINs(lAffinity.qProto("select * where \"a/b/c/d/e\"=3"))
    sql = "SELECT afy:pinID, \"a/b/c/d/e\" FROM " + str(pin[0].mPID)
    lAffinity.q(sql)
    assert lAffinity.qCount(sql) == 1
    lAffinity.setPrefix("q", "a/b/c/d/")
    sql = "SELECT afy:pinID, q:e FROM " + str(pin[0].mPID)
    lAffinity.q(sql)
    assert lAffinity.qCount(sql) == 1
    lAffinity.close()
    return True

def test_case_10():
    # for reproducing bug#348
    lAffinity = AFFINITY()
    lAffinity.open()
    lAffinity.q("CREATE CLASS board AS SELECT * WHERE EXISTS(food);")
    lAffinity.q("INSERT food={\'111101111111\'}, width=4, height=3;")
    lAffinity.q("CREATE CLASS c_328 AS SELECT * WHERE EXISTS(signal) SET afy:onEnter=${UPDATE board ADD food=SUBSTR(food[:LAST], 0, 2) || \'0\' || SUBSTR(food[:LAST], 3, width * height - 3)};")
    lAffinity.q("INSERT signal=1")
    lAffinity.close()
    return True

def test_case_11():
    # for reproducing bug#350
    lAffinity = AFFINITY()
    lAffinity.open()
    lAffinity.q("INSERT case11_a=1, case11_b=(INSERT afy:predicate=${SELECT * WHERE EXISTS(toto)},afy:objectID=.case11_c1);")
    lAffinity.q("INSERT case11_a=1, case11_b=(INSERT afy:predicate=${SELECT * WHERE EXISTS(toto)},afy:objectID=\'case11_c2\');")
    lAffinity.q("INSERT case11_a=1, case11_b=(CREATE CLASS case11_c3 AS SELECT * WHERE EXISTS(toto));")
    assert lAffinity.qCount("select * where exists(case11_a)") == 3
    lAffinity.close()
    return True

def test_case_12():
    # for reproducing comment 3, bug#178 
    lAffinity = AFFINITY()
    lAffinity.open()
    lAffinity.q("INSERT mylength=10m;")
    lAffinity.q("SELECT CAST(mylength AS ft) WHERE EXISTS(mylength);")
    lAffinity.q("SELECT CAST(mylength AS in) WHERE EXISTS(mylength);")
    # TODO: How to verify the result of CAST() ?
    lAffinity.close()
    return True

def test_case_13():
    # basic test case for enumeration
    lAffinity = AFFINITY()
    lAffinity.open()
    lAffinity.q("CREATE ENUMERATION WEEK AS {'SUNDAY', 'MONDAY', 'TUESDAY', 'WEDNESDAY', 'THURSDAY', 'FRIDAY', 'SATURDAY'};")
    lAffinity.q("INSERT (basic_t13_p1, basic_t13_p2) VALUES ('2012-11-26', WEEK#MONDAY)")
    lAffinity.q("INSERT (basic_t13_p1, basic_t13_p2) VALUES ('2012-12-10', WEEK#MONDAY)")
    lAffinity.q("INSERT (basic_t13_p1, basic_t13_p2) VALUES ('2012-12-11', WEEK#TUESDAY)")
    lAffinity.q("INSERT (basic_t13_p1, basic_t13_p2) VALUES ('2012-12-12', WEEK#WEDNESDAY)")
    lAffinity.q("INSERT (basic_t13_p1, basic_t13_p2) VALUES ('2012-12-13', WEEK#THURSDAY)")
    lAffinity.q("INSERT (basic_t13_p1, basic_t13_p2) VALUES ('2012-12-14', WEEK#FRIDAY)")
    lAffinity.q("INSERT (basic_t13_p1, basic_t13_p2) VALUES ('2012-12-15', WEEK#SATURDAY)")
    lAffinity.q("CREATE CLASS basic_t13_c1 AS SELECT * WHERE EXISTS(basic_t13_p2)")
    
    assert lAffinity.qCount("SELECT * FROM basic_t13_c1 WHERE basic_t13_p2 = WEEK#TUESDAY;") == 1
    assert lAffinity.qCount("SELECT * FROM basic_t13_c1 WHERE basic_t13_p2 = WEEK#MONDAY;") == 2

    # enumeration comparison
    assert lAffinity.qCount("SELECT * FROM basic_t13_c1 WHERE basic_t13_p2 > WEEK#WEDNESDAY;") == 3
    assert lAffinity.qCount("SELECT * FROM basic_t13_c1 WHERE basic_t13_p2 >= WEEK#WEDNESDAY;") == 4
    assert lAffinity.qCount("SELECT * FROM basic_t13_c1 WHERE basic_t13_p2 < WEEK#WEDNESDAY;") == 3
    assert lAffinity.qCount("SELECT * FROM basic_t13_c1 WHERE basic_t13_p2 <= WEEK#WEDNESDAY;") == 4
    assert lAffinity.qCount("SELECT * FROM basic_t13_c1 WHERE basic_t13_p2 <> WEEK#WEDNESDAY;") == 6
    
    # for reproducing a bug related to enumeration
    lAffinity.q("SET PREFIX basic_t13_pfx: 'abcd://dec/ccc/';CREATE ENUMERATION basic_t13_pfx:\"bla/bla/bla\" AS {'a/a', 'b/b', 'c/c'};")
    lAffinity.q("SET PREFIX basic_t13_pfx: 'abcd://dec/ccc/';INSERT basic_t13_toto=basic_t13_pfx:\"bla/bla/bla\"#\"a/a\"")
    assert lAffinity.qCount("SELECT * WHERE EXISTS(basic_t13_toto)") == 1
    
    lAffinity.close()
    return True

def test_case_14():
    # for reproducing bug#371
    lAffinity = AFFINITY()
    lAffinity.open()

    # this statement should returns error
    # lAffinity.q("CREATE CLASS t14_c1 AS SELECT * WHERE EXISTS(bla) SET afy:onEnter={${INSERT v=1}${INSERT b=2}}")

    lAffinity.q("CREATE CLASS t14_c2 AS SELECT * WHERE EXISTS(t14_bla) SET afy:onEnter={${INSERT t14_v=1},${INSERT t14_b=2}}")
    lAffinity.q("INSERT t14_bla = 13");

    assert lAffinity.qCount("SELECT * WHERE EXISTS(t14_v)") == 1
    assert lAffinity.qCount("SELECT * WHERE EXISTS(t14_b)") == 1
        
    lAffinity.close()
    return True

def test_case_15():
    # for reproducing bug#373
    lAffinity = AFFINITY()
    lAffinity.open()

    lAffinity.q("INSERT t15_children={(INSERT t15_val=1),(INSERT t15_val=2),(INSERT t15_val=3),(INSERT t15_val=4)}")
    lAffinity.q("CREATE CLASS t15_c1 AS SELECT * WHERE EXISTS(t15_val)")

    assert lAffinity.qCount("SELECT * FROM t15_c1 WHERE t15_val IN {1,2,10,20}") == 2
    assert lAffinity.qCount("SELECT * FROM t15_c1 WHERE t15_val NOT IN {1,2,10,20}") == 2
    lAffinity.close()
    return True

def test_case_16():
    # for reproducing bug#364 and #363
    lAffinity = AFFINITY()
    lAffinity.open()

    lAffinity.q("CREATE CLASS t16_c1 AS SELECT * WHERE t16_mytype='c1'")
    lAffinity.q("CREATE CLASS t16_c2 AS SELECT * WHERE t16_mytype='c2'")
    lAffinity.q("INSERT t16_mytype='c1', x=5")
    lAffinity.q("INSERT t16_mytype='c1', x=15")
    lAffinity.q("INSERT t16_mytype='c1', x=25")
    lAffinity.q("INSERT t16_mytype='c2', x=2")
    lAffinity.q("INSERT t16_mytype='c2', x=12")
    lAffinity.qProto("SELECT membership(@) FROM *")
    lAffinity.qProto("SELECT membership(@) WHERE EXISTS(x)")
    lAffinity.qProto("SELECT membership(@) FROM t16_c1")
    lAffinity.qProto("SELECT * FROM t16_c1 UNION SELECT * FROM t16_c2")
    lAffinity.qProto("SELECT * FROM t16_c1 WHERE x>10 UNION SELECT * FROM t16_c2 WHERE x<5")

    # Todo : verify

    lAffinity.close()
    return True

def test_case_17():
    # for reproducing bug#370
    lAffinity = AFFINITY()
    lAffinity.open()

    lAffinity.q("INSERT t17_v=1;")
    lAffinity.q("INSERT t17_v=2;")
    lAffinity.q("INSERT t17_v=3;")
    lAffinity.q("INSERT t17_v=4;")
    lAffinity.q("INSERT t17_v=5;")
    lAffinity.q("INSERT t17_v=6;")
    lAffinity.q("CREATE CLASS t17_c1 AS SELECT * WHERE EXISTS(t17_v)")
    lAffinity.q("CREATE CLASS t17_c2 AS SELECT * WHERE t17_v>3;")
    assert lAffinity.qCount("SELECT * FROM t17_c2") == 3
    lAffinity.q("SELECT * FROM t17_c1 EXCEPT SELECT * FROM t17_c2;")
    assert lAffinity.qCount("SELECT * FROM t17_c1 EXCEPT SELECT * FROM t17_c2;") == 3
    # bug#370 comment#1 : below statement is not implemented yet. 
    #lPins = PIN.loadPINs(lAffinity.qProto("SELECT t17_v FROM t17_c1 EXCEPT SELECT t17_v FROM t17_c2;"))
    #for i in range(0,3):
    #    assert lPins[i]['t17_v']<=3
    lAffinity.q("CREATE CLASS t17_c3 AS SELECT * WHERE EXISTS(t17_signal) SET afy:onEnter=${UPDATE @self SET t17_vv=(SELECT t17_v FROM t17_c1 EXCEPT SELECT t17_v FROM t17_c2)};")

    # for reproducing bug#392
    lPins =  PIN.loadPINs(lAffinity.qProto("CREATE CLASS \"http://bla/bla\" AS SELECT * WHERE EXISTS(toto)"))
    assert cmp(lPins[0]['afy:objectID'],'http://bla/bla') == 0

    # for reproducing bug#382
    lAffinity.q("INSERT afy:service=.srv:IO, afy:address=2")
    lAffinity.q("INSERT t17_bla=123")
    lAffinity.q("SELECT * WHERE EXISTS(t17_bla)")

    lAffinity.close()
    return True

def test_case_18():
    # for reproducing bug#323
    lAffinity = AFFINITY()
    lAffinity.open()
    
    lAffinity.q("INSERT @:1 T18_SELF_REF=10, T18_ref=(INSERT T18_SELF_REF=11, T18_ref=@:1);") 
    lAffinity.q("INSERT @:1 T18_SELF_REF=20, T18_ref=(INSERT @:2 T18_SELF_REF=21, T18_ref=@:1, T18_ref1=@:2)")
    lAffinity.q("INSERT @:1 T18_SELF_REF=30, T18_ref={(INSERT T18_SELF_REF=31, T18_ref=@50002), (INSERT SELF_REF=32, ref=@50003)}")
    lAffinity.q("INSERT @:1 T18_SELF_REF=30, ref={(INSERT T18_SELF_REF=31, T18_ref=@:1), (INSERT T18_SELF_REF=32, T18_ref=@:1)};")
    lAffinity.close()
    return True

def test_case_19():
    # for reproducing bug#368
    lAffinity = AFFINITY()
    lAffinity.open()
    
    lAffinity.q("CREATE CLASS t19_myview AS SELECT * WHERE t19_myowner=:0") 
    lAffinity.q("INSERT t19_signal=1, t19_sender=(INSERT @:1 t19_name='Fred'),t19_bogus=(INSERT t19_myowner=@:1, t19_value=15)")
    lPins = PIN.loadPINs(lAffinity.qProto("SELECT * WHERE EXISTS(t19_myowner)"))
    print lPins[0]['t19_myowner']
    sql = "SELECT * WHERE t19_myowner=" + str(lPins[0]['t19_myowner'])
    assert lAffinity.qCount(sql) == 1
    assert lAffinity.qCount("SELECT * FROM t19_myview") == 1
    sql = "SELECT * FROM t19_myview WHERE t19_myowner=" + str(lPins[0]['t19_myowner'])
    assert lAffinity.qCount(sql) == 1
    # repro comment 3
    lAffinity.q("INSERT @:1 t19_bogus={(INSERT t19_myowner=1), (INSERT t19_myowner=@:1)}")    

    lAffinity.close()
    return True

def test_case_20():
    # for reproducing bug#367(part of)
    lAffinity = AFFINITY()
    lAffinity.open()
    
    lAffinity.q("INSERT t20_v=1;")
    lAffinity.q("INSERT t20_v=2;")
    lAffinity.q("INSERT t20_v=4;")
    lAffinity.q("INSERT SELECT 't20_bla' as t20_vv WHERE EXISTS(t20_v)")
    lAffinity.q("INSERT SELECT @self.t20_v as t20_vv WHERE EXISTS(t20_v)")

    lAffinity.close()
    return True

def test_case_21():
    # for reproducing bug#316
    lAffinity = AFFINITY()
    lAffinity.open()
    
    lAffinity.q("INSERT t21_a=1")
    lAffinity.q("INSERT t21_a=2")
    lAffinity.q("INSERT t21_a=3")

    lAffinity.q("CREATE CLASS t21_c AS SELECT * WHERE t21_a IN :0")
    lPins = PIN.loadPINs(lAffinity.qProto("SELECT t21_a FROM t21_c(1,2,3) ORDER BY t21_a"))
    print lPins
    #assert lPins[0]['t21_a'] == 1
    #assert lPins[1]['t21_a'] == 2
    lPins = PIN.loadPINs(lAffinity.qProto("SELECT t21_a FROM t21_c ORDER BY t21_a DESC"))
    print lPins
    #assert lPins[0]['t21_a'] == 3
    #assert lPins[2]['t21_a'] == 1
    lPins = PIN.loadPINs(lAffinity.qProto("SELECT t21_a FROM * WHERE EXISTS(t21_a) ORDER BY t21_a"))
    print lPins
    #assert lPins[0]['t21_a'] == 1
    #assert lPins[1]['t21_a'] == 2
    lAffinity.q("CREATE CLASS t21_d AS SELECT * WHERE EXISTS(t21_a);")
    lPins = PIN.loadPINs(lAffinity.qProto("SELECT t21_a FROM t21_d ORDER BY t21_a;"))
    print lPins
    #assert lPins[0]['t21_a'] == 1
    lPins = PIN.loadPINs(lAffinity.qProto("SELECT * FROM t21_d ORDER BY t21_a;"))
    print lPins
    assert lPins[0]['t21_a'] == 1

    lAffinity.close()
    return True    
                
def _entryPoint():
    # test cases are logged here
    test_cases = ['test_case_1','test_case_2','test_case_3','test_case_4','test_case_5','test_case_6','test_case_7',\
'test_case_8','test_case_9','test_case_10','test_case_11','test_case_12','test_case_13','test_case_14','test_case_15',\
'test_case_16','test_case_17','test_case_18','test_case_19', 'test_case_20', 'test_case_21']
    
    start = time.time()
    for i in range(0, len(test_cases)):
        eval(test_cases[i]+"()")
    end = time.time()
    print "TestBasicPathSQL costs : " + timeToString(end - start)
    
class TestBasicPathSQL(AffinityTest):
    def execute(self):
        _entryPoint()

AffinityTest.declare(TestBasicPathSQL)

if __name__ == '__main__':
    lT = TestBasicPathSQL()
    lT.execute()

