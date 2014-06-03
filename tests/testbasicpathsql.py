#!/usr/bintenv python2.6
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
    lAffinity.q("CREATE CLASS c7 AS SELECT * WHERE EXISTS(signal) SET machine=(SELECT * WHERE type=\'washer\'), afy:onEnter=${UPDATE @ctx ADD _home=(SELECT afy:pinID FROM homes WHERE @ctx.machine=machines)};")
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
    # Mark: At the moment a SELECT with constant list returns just once independent of WHERE, FROM, etc. It may change in the future.
    assert lAffinity.qCount("SELECT * WHERE EXISTS(t20_vv)") == 1
    lAffinity.q("INSERT SELECT t20_v as t20_vv1 WHERE EXISTS(t20_v)")
    assert lAffinity.qCount("SELECT * WHERE EXISTS(t20_vv1)") == 3

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

def test_case_22():
    # test for using name pin
    lAffinity = AFFINITY()
    lAffinity.open()

    lAffinity.clearPrefixes();                  
    lAffinity.setPrefix("p","http://myprefix")
    lAffinity.q("INSERT afy:objectID=.p:myobject, t22_myvalue=10;")
    lAffinity.q("UPDATE #p:myobject SET t22_myvalue=11");
    lPins = PIN.loadPINs(lAffinity.qProto("SELECT * WHERE afy:objectID=.p:myobject;"))
    assert lPins[0]['t22_myvalue'] == 11
    # again here I notice that the substitution is destructive... there's no memory of p:myobject in the timer... no big deal, but should document...
    lAffinity.q("CREATE TIMER t22_t1 INTERVAL '00:00:05' AS UPDATE #p:myobject SET t22_myvalue=t22_myvalue+1, t22_t=CURRENT_TIMESTAMP")
    lAffinity.q("CREATE CLASS t22_c1 AS SELECT * WHERE EXISTS(t22_signal) SET afy:onEnter=${UPDATE #p:myobject SET t22_myvalue=t22_myvalue+@self.t22_v, t22_t=CURRENT_TIMESTAMP}")
    lAffinity.q("INSERT t22_signal=15, t22_v=123")

    lAffinity.q("CREATE CLASS t22_class1 AS select * where EXISTS(prop1)")
    assert lAffinity.qCount("SELECT * FROM afy:Classes WHERE CONTAINS(afy:objectID, 'class1')") == 1   
    assert lAffinity.qCount("SELECT * FROM afy:Classes WHERE CONTAINS(afy:objectID, 't22_class1')") == 1   
    assert lAffinity.qCount("SELECT * FROM afy:Classes WHERE CONTAINS(afy:objectID, 'class2')") == 0

    lAffinity.close()
    return True

def test_case_23():
    #for reproducing bug#434
    lAffinity = AFFINITY()
    lAffinity.open()
    
    lAffinity.setPrefix("control","http://example/control")
    lAffinity.setPrefix("simulation","http://example/simulation")
    # Declare a base class of signalable entities, triggered by single timer.
    lAffinity.q("CREATE CLASS control:\"rt/signalable\" AS SELECT * WHERE EXISTS(control:\"rt/time/signal\")")
    # Declare a sub-class with a specific event handler.
    lAffinity.q("CREATE CLASS control:\"step/handler/on.off.572ef13c\" AS SELECT * FROM control:\"rt/signalable\" WHERE control:\"sensor/model\"=.simulation:\"sensor/on.off.572ef13c\" SET afy:onUpdate={${UPDATE @ctx SET tmp1=(SELECT control:\"rt/time/signal\" FROM @self)},${INSERT simulation:\"rt/value\"=(SELECT simulation:\"offset/value\" FROM @self), control:\"sensor/model\"=(SELECT control:\"sensor/model\" FROM @self), control:handler=(SELECT afy:objectID FROM @ctx), control:at=CURRENT_TIMESTAMP}}")
    # Declare a few signalable entities.
    lAffinity.q("INSERT control:\"rt/time/signal\"=0, control:\"sensor/name\"='sensor A',control:\"sensor/model\"=.simulation:\"sensor/on.off.572ef13c\",simulation:\"offset/value\"=100")
    lAffinity.q("INSERT control:\"rt/time/signal\"=0,control:\"sensor/name\"='sensor B',control:\"sensor/model\"=.simulation:\"sensor/on.off.572ef13c\",simulation:\"offset/value\"=1000")
    # Trigger all signalable entities.
    lAffinity.q("CREATE TIMER control:\"rt/source/timer\" INTERVAL '00:00:01' AS UPDATE control:\"rt/signalable\" SET control:\"rt/time/signal\"=EXTRACT(SECOND FROM CURRENT_TIMESTAMP), control:\"rt/time\"=CURRENT_TIMESTAMP")

    lAffinity.close()
    return True

def test_case_24():
    #for reproducing bug#431
    lAffinity = AFFINITY()
    lAffinity.open()
    
    lAffinity.q("INSERT t24_bla=1")
    lAffinity.q("CREATE CLASS t24_c1 AS SELECT * WHERE EXISTS(t24_signal) SET afy:onEnter=${UPDATE * SET t24_x=@self.bogus, t24__at=CURRENT_TIMESTAMP WHERE EXISTS(t24_bla)}")
    lAffinity.q("INSERT t24_signal=10")
    assert lAffinity.qCount("SELECT * WHERE EXISTS(t24_x)") == 0
    assert lAffinity.qCount("SELECT * WHERE EXISTS(t24__at)") == 1
    
    lAffinity.close()
    return True

def test_case_25():
    #for reproducing bug#432
    lAffinity = AFFINITY()
    lAffinity.open()
    
    lAffinity.clearPrefixes();
    lAffinity.setPrefix("model","http://example/model/")
    lAffinity.q("CREATE CONDITION model:OutsideTmpChk AS model:OutsideTemp > :0")
    lAffinity.q("CREATE CONDITION model:InsideTmpChk AS (SELECT ABS(AVG(model:InsideTempReadings) - :0)) > 5dC")
    lAffinity.q("CREATE ACTION model:Pause AS UPDATE @self SET model:PausedAt=CURRENT_TIMESTAMP + :0;")
    lAffinity.q("RULE model:HeatAlarm : model:OutsideTmpChk(25dC) AND model:InsideTmpChk(20dC) ->  model:Pause(INTERVAL'00:15:00');")
    
    # TODO: insert pins and verify
    lAffinity.close()
    return True

def test_case_26():
    #for reproducing bug#437
    lAffinity = AFFINITY()
    lAffinity.open()
    
    lAffinity.clearPrefixes();
    lAffinity.setPrefix("ext","ext")
    lAffinity.q("CREATE CLASS ext:nth AS SELECT * WHERE EXISTS(ext:\"nth/call\") SET afy:onEnter={${UPDATE @self SET ext:\"nth/result\"=(SELECT ext:\"nth/list\"[:FIRST] FROM @self) WHERE ext:\"nth/call\"=0},${UPDATE @self DELETE ext:\"nth/list\"[:FIRST]},${UPDATE @self SET ext:\"nth/inner\"= (INSERT SELECT ext:\"nth/list\", (ext:\"nth/call\" - 1) AS ext:\"nth/call\" FROM @self) WHERE ext:\"nth/call\" > 0},${UPDATE @self SET ext:\"nth/result\"= (SELECT ext:\"nth/result\" FROM @self.ext:\"nth/inner\") WHERE EXISTS(ext:\"nth/inner\")}};")

    lAffinity.q("INSERT ext:\"nth/list\"={'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z'}, ext:\"nth/call\"=2")
    
    lAffinity.q("INSERT ext:\"nth/list\"={'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z'}, ext:\"nth/call\"=10")

    lAffinity.close()
    return True

def test_case_27():
    #for reproducing bug#441
    lAffinity = AFFINITY()
    lAffinity.open()
    assert lAffinity.qCount("SELECT RAW * FROM afy:NamedObjects") ==  lAffinity.qCount("SELECT RAW * FROM \"http://affinityng.org/builtin/NamedObjects\"")
    assert lAffinity.qCount("SELECT RAW * FROM afy:Classes") == lAffinity.qCount("SELECT RAW * FROM \"http://affinityng.org/builtin/Classes\"")
    lAffinity.close()
    return True

def test_case_28():
    #for reproducing bug#417
    lAffinity = AFFINITY()
    lAffinity.open()
    
    lAffinity.clearPrefixes();
    lAffinity.q("INSERT @:1 @{SELF_REF=50, ref=(INSERT @:2 @{SELF_REF=31,ref=@:1})}")   
    #lAffinity.q("INSERT @:1 @{SELF_REF=50, ref=(@:2 @{SELF_REF=31, ref=@:1})}")

    lAffinity.close()
    return True

def test_case_29():
    #for reproducing bug#374
    lAffinity = AFFINITY()
    lAffinity.open()
    
    lAffinity.q("INSERT t29_ch={(INSERT t29_val=1), (INSERT t29_val=2), (INSERT t29_val=3)};")
    lAffinity.q("CREATE CLASS t29_c1 AS SELECT * WHERE EXISTS(t29_val)")

    lAffinity.q("CREATE CLASS t29_cc1 AS SELECT * WHERE EXISTS(t29_signal) SET afy:onEnter=${UPDATE @self SET res=(SELECT t29_val FROM t29_c1 WHERE t29_val=@self.t29_v)};")
    lAffinity.q("CREATE CLASS t29_cc2 AS SELECT * WHERE EXISTS(t29_signal) SET afy:onEnter=${UPDATE @self SET res=(SELECT t29_val FROM t29_c1 WHERE t29_val IN @self.t29_v)}")

    lAffinity.q("CREATE CLASS t29_cc3 AS SELECT * WHERE EXISTS(t29_signal) SET afy:onEnter=${UPDATE @self SET res=(SELECT t29_val FROM t29_c1 WHERE t29_val IN (SELECT t29_v FROM @self))}")

    lAffinity.q("INSERT t29_signal=1, t29_v={2,3,4,5};")

    lAffinity.close()
    return True

def test_case_30():
    #for reproducing bug#435
    lAffinity = AFFINITY()
    lAffinity.open()
    
    # comment 0
    lAffinity.q("CREATE CLASS t30_c1 AS SELECT * WHERE EXISTS(t30_signal) SET afy:onEnter={${UPDATE @auto SET t30_tmp1=123}}")
    lAffinity.q("INSERT t30_signal=1;")

    # comment 2
    lAffinity.q("CREATE CLASS t30_c2 AS SELECT * WHERE EXISTS(t30_signal) SET afy:onEnter={${UPDATE @auto SET t30_tmp1=@self.t30_signal * 1000},${UPDATE @self SET t30_result=@auto.t30_tmp1}}")
    lAffinity.q("INSERT t30_signal=1")    
    
    # comment 5
    lAffinity.q("CREATE CLASS t30_c3 AS SELECT * WHERE EXISTS(t30_whatever);")
    lAffinity.q("INSERT t30_whatever=1, t30_x=5, t30_y=50;")
    lAffinity.q("INSERT t30_whatever=2, t30_x=10, t30_y=100;")
    lAffinity.q("INSERT t30_whatever=3, t30_x=15, t30_y=150;")
    lAffinity.q("CREATE CLASS t30_c4 AS SELECT * WHERE EXISTS(t30_signal) SET afy:onEnter={${UPDATE @auto SET t30__ly=(SELECT t30_y FROM t30_c3 WHERE t30_x=@self.t30_x)},${UPDATE @self SET t30_y=@auto.t30__ly}};")
    lAffinity.q("INSERT t30_signal=1, t30_x=10;")

    # comment 8
    lAffinity.q("CREATE CLASS t30_c5 AS SELECT * WHERE EXISTS(t30_signal) SET afy:onEnter={${UPDATE @auto SET t30_s=@self.t30_signal},${UPDATE @auto SET t30_ss=t30_s+1}, ${UPDATE @self SET t30_ss=@auto.t30_ss}};")
    lAffinity.q("INSERT t30_signal=1;")

    # comment 10
    lAffinity.q("CREATE CLASS t30_c6 AS SELECT * WHERE EXISTS(t30_signal) SET afy:onEnter={${UPDATE @auto SET t30_default=1},${UPDATE @auto SET t30_default=CURRENT_TIMESTAMP WHERE @self.t30_signal > 10},${UPDATE @self SET t30_yipee=@auto.t30_default}};")
    lAffinity.q("INSERT t30_signal=100;")
    
    lAffinity.q("CREATE CLASS t30_c7 AS SELECT * WHERE EXISTS(t30_signal) SET afy:onEnter={${UPDATE @self SET t30_default=1},${UPDATE @self SET t30_default=CURRENT_TIMESTAMP WHERE t30_signal > 10},${UPDATE @self SET t30_yipee=t30_default},${UPDATE @self DELETE t30_default}};")
    lAffinity.q("INSERT t30_signal=100;")

    lAffinity.close()
    return True

def test_case_31():
    #for reproducing bug#436
    lAffinity = AFFINITY()
    lAffinity.open()
    
    lAffinity.clearPrefixes();
    lAffinity.setPrefix("abc", "http://example")
    lAffinity.q("INSERT afy:objectID=.abc:example1, t31_myvalue='Hello1';")
    lAffinity.q("INSERT afy:objectID='example2', t31_myvalue='Hello2';")
    lAffinity.q("SELECT * FROM #abc:example1;")
    lAffinity.q("SELECT * FROM #example2;")
    lAffinity.q("UPDATE #abc:example1 SET yetanothervalue='Yes indeed!';")
    lAffinity.q("SELECT * FROM #\"http://example/example1\";")
    lAffinity.q("UPDATE #\"http://example/example1\" SET someothervalue='World!';")

    lAffinity.close()
    return True

def test_case_32(): 
    # UPDATE #test EDIT SUBSTR(s,3,4)='dfgh, implemented in kernel rev.#1538
    # UPDATE ... SET a||='abc' (through OP_EDIT), implemented in kernel rev.#1539
    lAffinity = AFFINITY()
    lAffinity.open()
    
    lAffinity.clearPrefixes();
    lAffinity.q("INSERT afy:objectID='test32', t32_coll=1, t32_str='123456'")
    lAffinity.q("UPDATE #test32 EDIT SUBSTR(t32_str,1,2)='dfgh'")
    lPINs = PIN.loadPINs(lAffinity.qProto("SELECT * FROM #test32"))
    assert lPINs[0]['t32_str'] == '1dfgh456'
    lAffinity.q("UPDATE #test32 EDIT SUBSTR(t32_str,1,0)='abc'")
    lPINs[0].refreshPIN() 
    assert lPINs[0]['t32_str'] == '1abcdfgh456'
    lAffinity.q("UPDATE #test32 EDIT SUBSTR(t32_str,0,1)='abc'")
    lPINs[0].refreshPIN() 
    assert lPINs[0]['t32_str'] == 'abcabcdfgh456'

    lAffinity.q("UPDATE #test32 SET t32_str||='789'")
    lPINs[0].refreshPIN() 
    print lPINs[0]['t32_str']
    
    lAffinity.close()
    return True

def test_case_33():
    #for reproducing bug#446
    lAffinity = AFFINITY()
    lAffinity.open()
    
    lAffinity.q("CREATE CLASS t33_c1 AS SELECT * WHERE EXISTS(t33_signal) SET afy:onEnter=${INSERT SELECT afy:value AS t33_a FROM @[1, @self.t33_signal]}")
    lAffinity.q("INSERT t33_signal=1")
    pin = PIN.loadPINs(lAffinity.qProto("SELECT * WHERE EXISTS(t33_a)"))[0]
    assert pin['t33_a'] == 1

    lAffinity.close()
    return True

def test_case_34():
    #for reproducing bug#454
    lAffinity = AFFINITY()
    lAffinity.open()
    
    lAffinity.q("INSERT @:1 t34_children=(INSERT parents={@:1})")
    lAffinity.q("INSERT @:1 t34_children=(INSERT parents=@:1)")
    assert lAffinity.qCount("SELECT * WHERE EXISTS(t34_children)") == 2

    lAffinity.close()
    return True
    
def test_case_35():
    # for reproducing bug#450
    lAffinity = AFFINITY()
    lAffinity.open()

    lAffinity.q("CREATE CLASS t35_c2 AS SELECT * WHERE t35_signal2 IN :0;")
    lAffinity.q("CREATE CLASS t35_c1 AS SELECT * WHERE EXISTS(t35_signal1) SET afy:onEnter=${INSERT SELECT afy:value as t35_whatever, CURRENT_TIMESTAMP AS t35_signal2 FROM @[1, 20]};")
    lAffinity.q("INSERT t35_signal1=1;")
    
    assert lAffinity.qCount("SELECT * WHERE EXISTS(t35_signal1)") == 1
    assert lAffinity.qCount("SELECT * WHERE EXISTS(t35_whatever)") == 20
    
    pin = PIN.loadPINs(lAffinity.qProto("SELECT * FROM t35_c2 WHERE t35_signal2=(SELECT MIN(t35_signal2) FROM t35_c2)"))[0]
    assert pin['t35_whatever'] == 1

    lAffinity.close()
    return True
        
def test_case_36():
    # for reproducing bug#460
    lAffinity = AFFINITY()
    lAffinity.open()

    lAffinity.q("INSERT SELECT {.srv:protobuf, .srv:sockets} AS afy:service,'127.0.0.1:' || afy:value AS afy:address FROM @[8000,8001];")
    lAffinity.q("INSERT SELECT {.srv:protobuf, .srv:sockets} AS t36_bla,'127.0.0.1:' || afy:value AS afy:address FROM @[8000,8001];")

    lAffinity.close()
    return True

def test_case_37():
    # for reproducing bug#465
    lAffinity = AFFINITY()
    lAffinity.open()

    lAffinity.q("INSERT @{t37_v1=1, t37_v2=1}, @{t37_v1=2, t37_v2=2}, @{t37_v1=3, t37_v2=3};")
    lAffinity.q("CREATE CLASS t37_c AS SELECT * WHERE EXISTS(t37_v1);")
    lAffinity.q("INSERT afy:objectID='t37_test1', t37_theset=(SELECT * FROM t37_c);")
    lAffinity.q("SELECT t37_theset[:FIRST] FROM #t37_test1;")

    lAffinity.q("INSERT afy:objectID='t37_test2',t37_theset=(SELECT t37_v1,t37_v2 FROM t37_c);")
    # message corruption
    # pin = PIN.loadPINs(lAffinity.qProto("SELECT * FROM #t37_test2;"))[0]
    # print pin 
    lAffinity.q("SELECT * FROM #t37_test2;")
  
    lAffinity.q("SELECT t37_theset[:FIRST] FROM #t37_test2;")

    lAffinity.close()
    return True

def test_case_38():
    # testing OP_ARGMIN and OP_ARGMAX   
    lAffinity = AFFINITY()
    lAffinity.open()

    lAffinity.q("INSERT afy:objectID='t38_result'");

    # insert a collection
    lAffinity.q("INSERT afy:objectID='t38_case1', col={-1,7,8,1,-5,2,3}")
    
    lAffinity.q("UPDATE #t38_result SET result_min=(SELECT ARGMIN(col) FROM #t38_case1)");

    lAffinity.q("UPDATE #t38_result SET result_max=(SELECT ARGMAX(col) FROM #t38_case1)");

    pin = PIN.loadPINs(lAffinity.qProto("SELECT * FROM #t38_result"))[0]
    
    assert pin['result_min'] == 4

    assert pin['result_max'] == 2
    
    # insert another collection to test
    lAffinity.q("INSERT afy:objectID='t38_case2', col={1,1,1,1,1,1,1}")
    
    lAffinity.q("UPDATE #t38_result SET result_min = (SELECT ARGMIN(col) FROM #t38_case2)");

    lAffinity.q("UPDATE #t38_result SET result_max = (SELECT ARGMAX(col) FROM #t38_case2)");

    pin = PIN.loadPINs(lAffinity.qProto("SELECT * FROM #t38_result"))[0]
    
    assert pin['result_min'] == pin['result_max'] == 0

    lAffinity.close()
    return True

def test_case_39():
    # for reproducing bug#470

    lAffinity = AFFINITY()
    lAffinity.open()
    
    lAffinity.q("CREATE CLASS original_messages AS SELECT * WHERE mymessage IN :0 AND creator=8090 AND originator=creator AND NOT EXISTS (previous_originator)")
    assert 0 == lAffinity.qCount("SELECT * FROM original_messages")
 
    lAffinity.q("INSERT mymessage=(SELECT COUNT(*) FROM original_messages), creator=8090, originator=8090;")
    assert 1 == lAffinity.qCount("SELECT COUNT(*) FROM original_messages")
    
    assert 1 == lAffinity.qCount("SELECT mymessage FROM original_messages;") 
    lAffinity.q("INSERT mymessage=(SELECT COUNT(*) FROM original_messages), creator=8090, originator=8090;")
    assert 2 == lAffinity.qCount("SELECT mymessage FROM original_messages;") 

    # projection still doesn't work
    # pin = PIN.loadPINs(lAffinity.qProto("SELECT mymessage FROM original_messages;"))

    lAffinity.close()
    return True

def test_case_40():
    # testing SELECT f() FROM #XXX, repro bug#479

    lAffinity = AFFINITY()
    lAffinity.open()
    
    lAffinity.q("INSERT afy:objectID='t40_result'")
    lAffinity.q("INSERT afy:objectID='t40_ev1', userdata=1000, t40_f1=$(userdata * :0), t40_f2=$(userdata + :0)")
    lAffinity.q("UPDATE #t40_result SET t40_r1=(SELECT t40_f1(2) FROM #t40_ev1), t40_r2=(SELECT t40_f2(2) FROM #t40_ev1)")
    pin = PIN.loadPINs(lAffinity.qProto("SELECT * FROM #t40_result"))[0]
    
    assert pin['t40_r1'][0] == 2000
    assert pin['t40_r2'][0] == 1002
    
    lAffinity.close()
    return True    

def test_case_41():
    # testing select first
    lAffinity = AFFINITY()
    lAffinity.open()
    
    lAffinity.q("INSERT t41_p1=5")
    lAffinity.q("INSERT t41_p1=3")
    lAffinity.q("INSERT t41_p1=2")
    lAffinity.q("INSERT t41_p1=1")
    lAffinity.q("INSERT t41_p1=4")
    
    lAffinity.q("CREATE CLASS t41_c1 AS SELECT * WHERE EXISTS(t41_p1)")
    
    pin = PIN.loadPINs(lAffinity.qProto("SELECT FIRST WHERE EXISTS(t41_p1)"))[0]    
    print pin['t41_p1'] 
    
    pin = PIN.loadPINs(lAffinity.qProto("SELECT FIRST FROM t41_c1"))[0]
    print pin['t41_p1']

    pin = PIN.loadPINs(lAffinity.qProto("SELECT FIRST FROM t41_c1 WHERE t41_p1<4"))[0]
    assert pin['t41_p1'] == 3

    pin = PIN.loadPINs(lAffinity.qProto("SELECT FIRST FROM t41_c1 ORDER BY t41_p1"))[0]
    assert pin['t41_p1'] == 1

    pin = PIN.loadPINs(lAffinity.qProto("SELECT FIRST FROM t41_c1 ORDER BY t41_p1 DESC"))[0]
    assert pin['t41_p1'] == 5

    pin = PIN.loadPINs(lAffinity.qProto("SELECT FIRST FROM t41_c1  WHERE t41_p1 < 3 ORDER BY t41_p1 DESC"))[0]
    assert pin['t41_p1'] == 2
    
    lAffinity.close()
    return True        

def test_case_42():
    # testing OP_ELEMENT (e.g. SELECT x[y+1] WHERE EXISTS(x) AND EXISTS(y))
    lAffinity = AFFINITY()
    lAffinity.open() 
    
    lAffinity.q("INSERT afy:objectID='t42_result'")
    
    lAffinity.q("INSERT t42_p1={1,2,3,4,5,6,7,8}, t42_p2=1, afy:objectID='t42_case'")
    
    lAffinity.q("UPDATE #t42_result SET t42_result_p1=(SELECT t42_p1[t42_p2+1] WHERE EXISTS(t42_p1))")
    pin = PIN.loadPINs(lAffinity.qProto("SELECT * FROM #t42_result"))[0]
    assert pin['t42_result_p1'][0] == 3

    lAffinity.q("UPDATE #t42_result SET t42_result_p1=(SELECT t42_p1[t42_p2+3] FROM #t42_case)")
    pin = PIN.loadPINs(lAffinity.qProto("SELECT * FROM #t42_result"))[0]
    assert pin['t42_result_p1'][0] == 5

    lAffinity.q("CREATE CLASS t42_c1 AS SELECT * WHERE EXISTS(t42_p1)")    
    lAffinity.q("UPDATE #t42_result SET t42_result_p1=(SELECT t42_p1[t42_p2+6] FROM t42_c1)")
    pin = PIN.loadPINs(lAffinity.qProto("SELECT * FROM #t42_result"))[0]
    assert pin['t42_result_p1'][0] == 8

    lAffinity.q("CREATE CLASS t42_c2 AS SELECT * WHERE t42_p1[2] = 3")
    lAffinity.q("UPDATE #t42_result SET t42_result_p1=(SELECT t42_p1[t42_p2+6] FROM t42_c2)")
    pin = PIN.loadPINs(lAffinity.qProto("SELECT * FROM #t42_result"))[0]
    assert pin['t42_result_p1'][0] == 8

    lAffinity.q("CREATE CLASS t42_c3 AS SELECT * WHERE t42_p1[t42_p2+2] = 4")
    lAffinity.q("UPDATE #t42_result SET t42_result_p1=(SELECT t42_p1[1] FROM t42_c3)")
    pin = PIN.loadPINs(lAffinity.qProto("SELECT * FROM #t42_result"))[0]
    assert pin['t42_result_p1'][0] == 2

    lAffinity.close()
    return True    

def test_case_43():
    # for repro bug#485
    lAffinity = AFFINITY()
    lAffinity.open() 
    
    lAffinity.q("INSERT t43_prop_map={\'string\' -> X\'DEF5\', \'http://test/\' -> 128, 3.40282f -> 3.40282, true -> TIMESTAMP \'2010-12-31 23:59:59\'}")
    assert lAffinity.qCount("SELECT * WHERE EXISTS(t43_prop_map)") == 1
    
    lAffinity.close()
    return True 

def test_case_44():
    # for repro bug#487
    lAffinity = AFFINITY()
    lAffinity.open() 
    
    lAffinity.q("INSERT t44_x=10, t44_y=(SELECT * from afy:Classes WHERE EXISTS(t44_toto))")
    lAffinity.q("INSERT t44_x=10, t44_y=(SELECT * from afy:Classes WHERE afy:objectID='t44_toto')")
    lAffinity.q("INSERT t44_x=10, afy:objectID='t44_bla'")
    lAffinity.q("INSERT t44_x=20, t44_friend=(SELECT afy:pinID FROM #t44_bla)")
    
    assert lAffinity.qCount("SELECT * WHERE EXISTS(t44_x)") == 4

    lAffinity.close()
    return True 

def test_case_45():
    # for testing VT_ARRAY, two-dimensional arrays
    lAffinity = AFFINITY()
    lAffinity.open()
    
    lAffinity.q("INSERT afy:objectID='t45_result'")
    lAffinity.q("INSERT t45_a=[0,1,2,3,4,5,6,7,8,9],afy:objectID='t45_case1'")
    assert lAffinity.qCount("SELECT * WHERE EXISTS(t45_a)") == 1

    # add operation 
    lAffinity.q("UPDATE #t45_case1 SET t45_a=t45_a + [100,100,100,100,100,100,100,100,100,100]")
    lAffinity.q("UPDATE #t45_result SET result=(SELECT t45_a[1] FROM #t45_case1)")
    pin = PIN.loadPINs(lAffinity.qProto("SELECT * FROM #t45_result"))[0]
    assert pin['result'][0] == 101

    # substract operation
    lAffinity.q("UPDATE #t45_case1 SET t45_a=t45_a - [50,50,50,50,50,50,50,50,50,50]")
    lAffinity.q("UPDATE #t45_result SET result=(SELECT t45_a[3] FROM #t45_case1)")
    pin = PIN.loadPINs(lAffinity.qProto("SELECT * FROM #t45_result"))[0]
    assert pin['result'][0] == 53

    lAffinity.q("INSERT t45_b=[[0,1,2,3,4],[5,6,7,8,9]]")
    assert lAffinity.qCount("SELECT * WHERE EXISTS(t45_b)") == 1

    lAffinity.q("INSERT t45_c=[[1.1,2.2,33.3],[1.2,2.2,0.5]]")
    assert lAffinity.qCount("SELECT * WHERE EXISTS(t45_c)") == 1

    lAffinity.close()
    return True

def test_case_46():
    # for testing OP_NORM
    lAffinity = AFFINITY()
    lAffinity.open()
    
    lAffinity.q("INSERT afy:objectID='t46_result'")
    lAffinity.q("INSERT t46_p1=[3,4],afy:objectID='t46_case1'")
    lAffinity.q("UPDATE #t46_result SET result=(SELECT NORM(t46_p1) FROM #t46_case1)")
    pin = PIN.loadPINs(lAffinity.qProto("SELECT * FROM #t46_result"))[0]
    assert norm([3,4],0) == pin['result'][0]
    
    lAffinity.q("UPDATE #t46_result SET result=(SELECT NORM(t46_p1,1) FROM #t46_case1)")
    pin = PIN.loadPINs(lAffinity.qProto("SELECT * FROM #t46_result"))[0]
    assert norm([3,4],1) == pin['result'][0]
    
    lAffinity.q("UPDATE #t46_result SET result=(SELECT NORM(t46_p1,3) FROM #t46_case1)")
    pin = PIN.loadPINs(lAffinity.qProto("SELECT * FROM #t46_result"))[0]
    assert norm([3,4],3) == pin['result'][0]    
    
    # another array to test
    lAffinity.q("UPDATE #t46_case1 SET t46_p1=[3,-4,-1,2,7]")
    lAffinity.q("UPDATE #t46_result SET result=(SELECT NORM(t46_p1,2) FROM #t46_case1)")
    pin = PIN.loadPINs(lAffinity.qProto("SELECT * FROM #t46_result"))[0]
    assert norm([3,-4,-1,2,7],2) == pin['result'][0]
    
    lAffinity.q("UPDATE #t46_result SET result=(SELECT NORM(t46_p1,1) FROM #t46_case1)")
    pin = PIN.loadPINs(lAffinity.qProto("SELECT * FROM #t46_result"))[0]
    assert norm([3,-4,-1,2,7],1) == pin['result'][0]
    
    lAffinity.q("UPDATE #t46_result SET result=(SELECT NORM(t46_p1,3) FROM #t46_case1)")
    pin = PIN.loadPINs(lAffinity.qProto("SELECT * FROM #t46_result"))[0]
    assert norm([3,-4,-1,2,7],3) == pin['result'][0]

    # another double array to test
    lAffinity.q("UPDATE #t46_case1 SET t46_p1=[1000.8,-121.4,-122454.6,224234.12,733.1,24332.8,0.0]")   
    lAffinity.q("UPDATE #t46_result SET result=(SELECT NORM(t46_p1,2) FROM #t46_case1)")
    pin = PIN.loadPINs(lAffinity.qProto("SELECT * FROM #t46_result"))[0]
    assert norm([1000.8,-121.4,-122454.6,224234.12,733.1,24332.8,0],2) == pin['result'][0]

    lAffinity.q("UPDATE #t46_result SET result=(SELECT NORM(t46_p1,1) FROM #t46_case1)")
    pin = PIN.loadPINs(lAffinity.qProto("SELECT * FROM #t46_result"))[0]
    assert norm([1000.8,-121.4,-122454.6,224234.12,733.1,24332.8,0],1) == pin['result'][0]
    
    lAffinity.q("UPDATE #t46_result SET result=(SELECT NORM(t46_p1,3) FROM #t46_case1)")
    pin = PIN.loadPINs(lAffinity.qProto("SELECT * FROM #t46_result"))[0]
    assert norm([1000.8,-121.4,-122454.6,224234.12,733.1,24332.8,0],3) == pin['result'][0]

    lAffinity.q("UPDATE #t46_result SET result=(SELECT NORM(t46_p1,4) FROM #t46_case1)")
    pin = PIN.loadPINs(lAffinity.qProto("SELECT * FROM #t46_result"))[0]
    assert norm([1000.8,-121.4,-122454.6,224234.12,733.1,24332.8,0],4) == pin['result'][0]

    lAffinity.close()
    return True

def test_case_47():
    # for repro bug#494
    lAffinity = AFFINITY()
    lAffinity.open(pKeepAlive=True)

    lAffinity.clearPrefixes();   
    lAffinity.setPrefix("alrm","http://example/alarm-system")
    lAffinity.q("CREATE ENUMERATION alrm:DOOR_STATES AS {'OPEN', 'CLOSED'}")
    lAffinity.q("CREATE CLASS alrm:components AS SELECT * WHERE alrm:\"component/id\" IN :0")
    lAffinity.q("CREATE CLASS alrm:homes AS SELECT * WHERE alrm:\"home/id\" IN :0")
    lAffinity.q("INSERT alrm:\"home/id\"='C147'")
    lAffinity.q("UPDATE alrm:homes('C147') ADD alrm:doors=(INSERT alrm:\"door/id\"=1, alrm:\"door/state\"=alrm:DOOR_STATES#CLOSED)")
    lAffinity.q("UPDATE alrm:homes('C147').alrm:doors SET alrm:\"door/state\"=alrm:DOOR_STATES#OPEN")
    lAffinity.q("UPDATE alrm:homes('C147').alrm:doors SET alrm:\"door/state\"=alrm:DOOR_STATES#OPEN WHERE alrm:\"door/id\"=1")

    lAffinity.close()
    return True   

def test_case_48():
    # for repro bug#496
    lAffinity = AFFINITY()
    lAffinity.open()
    
    lAffinity.q("CREATE CLASS houses AS SELECT * WHERE houseid IN :0")
    lAffinity.q("INSERT houseid=1, doors={(INSERT doorid=1, color='green'),(INSERT doorid=2, color='blue'),(INSERT doorid=3, color='red')};")
    assert 1 == lAffinity.qCount("SELECT color FROM houses(1).doors WHERE doorid=1")
    assert 1 == lAffinity.qCount("SELECT color FROM houses(1).doors WHERE doorid=2;")
    assert 3 == lAffinity.qCount("SELECT color FROM houses(1).doors")
    assert 3 == lAffinity.qCount("SELECT color FROM houses.doors;") 

    lAffinity.close()
    return True  

def _entryPoint():
    # test cases are executed here
    start = time.time()
    TEST = 'test_case_'
    cnt = 0
    for i in range(1, 49):
        eval(TEST+str(i)+'()')
        cnt+=1
    
    end = time.time()
    print "Totally " + str(cnt) + " cases have been tested."
    print "TestBasicPathSQL costs : " + timeToString(end - start)
    
class TestBasicPathSQL(AffinityTest):
    def execute(self):
        _entryPoint()

AffinityTest.declare(TestBasicPathSQL)

if __name__ == '__main__':
    lT = TestBasicPathSQL()
    lT.execute()

