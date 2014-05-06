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
"""This test case is to test Affinity's online materials, including executable doc and tutorial."""

from testfwk import AffinityTest
from affinity import *
from utils import *
import datetime
import random
import string
import math
import os
import re

# remove html tags in line, return pathsql statement
def extract_pathsql_statement(line):
    line = line.strip();
    # remove header/ending tags, remove <br>
    p1 = re.compile(r'(><code.*t\'>)|(</code></p)|(<br>)')
    line = p1.sub('', line)
    # &quot; to "
    p2 = re.compile('&quot;')
    line = p2.sub('"', line)
    # &gt; to >
    p3 = re.compile('&gt;')
    line = p3.sub('>', line)
    return line

def test_executable_doc():
    lAffinity = AFFINITY()
    lAffinity.open()
    SQLs = []
    # read PathSQL statements in doc/pathSQL primer.md,
    # execute them one by one
    name = "../server/src/www/doc/pathSQL basics [control].html"
    execDoc = open(name, "r")
    PATHSQL = re.compile(r'.*pathsql_snippet.*')
    lines = execDoc.readlines()
    for line in lines:
        if (PATHSQL.match(line)):
            sql = extract_pathsql_statement(line)
            SQLs.append(sql)
    execDoc.close()
    
    for sql in SQLs:
        try:
            ret = lAffinity.q(sql);
        except Exception, ex:
            print sql
            print repr(ex)
            
    lAffinity.close()
    print "Test executable doc complete!"
    return True

def test_tutorial():
    lAffinity = AFFINITY()
    lAffinity.open()
    # run steps in console.html

    # step1
    # object = {name:'Jack', profession:'doctor'} for (i in object) { print(i + ":" + object[i]); }
    print "Start step1"
    obj = PIN({'name':'Jack', 'profession':'doctor'})
    print obj
    print "Step1 complete!"
	
    # step2
    # pins = pathsql("INSERT name='Steve', profession='engineer';");
    # print("resulting pin id:" + pins[0].id);
    # q("SELECT * FROM @" + pins[0].id + ";"); print("&lt;br&gt;");
    # q("SELECT * WHERE EXISTS(name) MATCH AGAINST('Ste');");
    print "Start step2"
    pins = PIN({'name':'Steve', 'profession':'engineer'}).savePIN()   
    print "resulting pin id : " + str(pins.mPID)    
    sql = "SELECT * FROM " + str(pins.mPID) + ";"
    ret = PIN.loadPINs(lAffinity.qProto(sql))
    print ret[0]['name']
    assert(ret[0]['name'] == "Steve")
    sql = "SELECT * WHERE EXISTS(name) MATCH AGAINST('Ste');"
    ret = lAffinity.q(sql)
    assert(ret != None)
    print "Step2 complete!" 
    
    # step3
    # q("INSERT name='John', profession='lawyer', age=37, friends={(INSERT name='Fred', profession='musician', age=17), (INSERT name='Stephanie', profession='surgeon', age=45), (INSERT name='Claire', profession='teacher', age=67)};") ;
    # q("SELECT * WHERE EXISTS(name);");
    print "Start step3"
    sql = "INSERT name='John', profession='lawyer', age=37, friends={(INSERT name='Fred', profession='musician', age=17), (INSERT name='Stephanie', profession='surgeon', age=45), (INSERT name='Claire', profession='teacher', age=67)};"
    lAffinity.q(sql);
    sql = "SELECT * WHERE EXISTS(name);"
    ret = PIN.loadPINs(lAffinity.qProto(sql))
    assert(len(ret) > 0)
    print "returned pins number : " + str(len(ret))
    print "Step3 complete!" 

    # step4
    # if (object != undefined) { print(object); save(object); print(object); }
    print "Start step4"
    if (obj != None):
        print(obj)
        lObj = obj.savePIN()
        assert lObj != None
        print(lObj)   
    print "Step4 complete!"

    # step5
    # if (object != undefined)
    # { object.mystring = 'hello world'; object.mycollection = [1, 2, 3, 4, 5]; object.mydate = new Date(); save(object); }
    # q("SELECT * WHERE EXISTS(mystring);");
    print "Start step5"
    if lObj != None:
        lObj['mystring'] = 'hello world'
        lObj['mycollection'] = [1, 2, 3, 4, 5]
        lObj['mydate'] = datetime.datetime.now()
        lObj = obj.savePIN()
    ret = PIN.loadPINs(lAffinity.qProto("SELECT * WHERE EXISTS(mystring);"))
    assert(len(ret) > 0)
    print "Step5 complete!" 

    # step6
    # names=['Abby', 'Bruce', 'Camille', 'Derek', 'Ed', 'Frank', 'Greg', 'Harold', 'Ivy', 'Joseph', 'Karl', 'Lynne', 'Mark', 'Nick', 'Oscar', 'Pam', 'Queen', 'Ramon', 'Steve', 'Ted', 'Ursula', 'Victor', 'Wayne', 'Xavier', 'Young', 'Zack'];
    # professions=['teacher', 'accompanist', 'accountant', 'actor', 'athlete', 'attorney', 'bacteriologist', 'botanist', 'conciliator', 'engineer', 'journalist', 'mathematician', 'mediator', 'meteorologist', 'salesman'];
    # pickFrom = function(array) { return array[Math.floor(Math.random() * array.length)]; };
    # statements = []; for (i = 0; i < 40; i++) statements.push("INSERT name='" + pickFrom(names) + "', age=" + Math.random()*100 + ", profession='" + pickFrom(professions) + "';");
    # q("START TRANSACTION;");
    # for (i = 0; i < statements.length; i++) { q(statements[i]); }
    # q("COMMIT;");
    print "Start step6"
    names = ['Abby', 'Bruce', 'Camille', 'Derek', 'Ed', 'Frank', 'Greg', 'Harold', 'Ivy', 'Joseph', 'Karl', 'Lynne', 'Mark', 'Nick', 'Oscar', 'Pam', 'Queen', 'Ramon', 'Steve', 'Ted', 'Ursula', 'Victor', 'Wayne', 'Xavier', 'Young', 'Zack']
    professions=['teacher', 'accompanist', 'accountant', 'actor', 'athlete', 'attorney', 'bacteriologist', 'botanist', 'conciliator', 'engineer', 'journalist', 'mathematician', 'mediator', 'meteorologist', 'salesman']
    pickFrom = lambda length: int(math.floor(random.random() * length))
    statements = []    
    for i in range(40):
        statements.append("INSERT name='" + names[pickFrom(len(names))] + "', age=" + str(random.random()*100) + ", profession='" + professions[pickFrom(len(professions))] + "';")
    lAffinity.startTx()
    for stmt in statements:
        lAffinity.q(stmt)
    lAffinity.commitTx()
    print "Step6 complete!" 

    # step7
    # q("CREATE CLASS workers AS SELECT * WHERE EXISTS(name) AND profession IN :0;");
    # q("SELECT * FROM workers;"); 
    # q("SELECT * FROM workers('musician');");
    # q("SELECT * FROM workers(@['l', 'n']);"); 
    # q("CREATE CLASS people AS SELECT * WHERE name IN :0;"); 
    # q("SELECT * FROM people(@['A', 'G']);");
    print "Start step7"
    lAffinity.q("CREATE CLASS workers AS SELECT * WHERE EXISTS(name) AND profession IN :0;")
    lAffinity.q("SELECT * FROM workers;")
    lAffinity.q("SELECT * FROM workers('musician');")
    lAffinity.q("SELECT * FROM workers(@['l', 'n']);")
    lAffinity.q("CREATE CLASS people AS SELECT * WHERE name IN :0;")
    lAffinity.q("SELECT * FROM people(@['A', 'G']);")
    print "Step7 complete!" 

    # step8
    # pins = q("SELECT afy:pinID FROM workers;");
    # pickFriend = function() { return Math.floor(Math.random() * pins.length); };
    # statements = []; for (i = 0; i < (3 * pins.length / 2); i++) { from = pins[pickFriend()]['afy:pinID']['$ref']; to = pins[pickFriend()]['afy:pinID']['$ref']; if (from != to) statements.push("UPDATE @" + from + " ADD friends=@" + to + " WHERE (NOT (@" + to + " = friends));"); }
    # q("START TRANSACTION;");
    # for (i = 0; i < statements.length; i++) { q(statements[i]); }
    # q("COMMIT;");
    print "Start step8"
    #lRaw = lAffinity.q("SELECT afy:pinID FROM workers;")
    #lPBStream = affinity_pb2.AfyStream()
    # TODO: investigate why ParseFromString() failed
    #lPBStream.ParseFromString(lRaw) 
    #pins = lPBStream.pins
    pins = PIN.loadPINs(lAffinity.qProto("SELECT * FROM workers;"))
    pickFriend = lambda length: int(math.floor(random.random() * length))
    statements = []
    for i in range(3*len(pins)/2):
        pFrom = pins[pickFriend(len(pins))].mPID
        pTo = pins[pickFriend(len(pins))].mPID
        if (pFrom != pTo):
            statements.append("UPDATE " + str(pFrom) + " ADD friends=" + str(pTo) + " WHERE (NOT (" + str(pTo) + " = friends));")
    lAffinity.startTx()
    for stmt in statements:
        lAffinity.q(stmt)
    lAffinity.commitTx()
    print "Step8 complete!" 

    # step9
    # colors=['amber', 'beige', 'carmine', 'denim', 'emerald', 'fuchsia', 'gold', 'ivory', 'jade', 'khaki', 'lapis lazuli', 'magenta', 'orange', 'pink', 'red', 'salmon', 'turquoise', 'vermilion', 'white', 'yellow'];
    # makes=['Audi', 'Bentley', 'DaimlerChrysler', 'Ferrari', 'GM', 'Honda', 'Jaguar', 'KIA', 'Lamborghini', 'Maserati', 'Nissan', 'Porsche', 'Rolls-Royce', 'Suzuki', 'Toyota', 'Volvo'];
    # pins = q("SELECT afy:pinID FROM workers;");
    # pickFrom = function(array) { return array[Math.floor(Math.random() * array.length)]; };
    # statements = []; for (i = 0; i < pins.length; i++) { statements.push("UPDATE @" + pickFrom(pins)['afy:pinID']['$ref'] + " ADD cars=(INSERT car_make='" + pickFrom(makes) + "', color='" + pickFrom(colors) + "', year='" + (1950 + Math.floor(Math.random() * 62)) + "');"); }
    # q("START TRANSACTION;");
    # for (i = 0; i < statements.length; i++) { q(statements[i]); }
    # q("COMMIT;");
    # q("CREATE CLASS cars AS SELECT * WHERE EXISTS(car_make);")
    print "Start step9"
    colors=['amber', 'beige', 'carmine', 'denim', 'emerald', 'fuchsia', 'gold', 'ivory', 'jade', 'khaki', 'lapis lazuli', 'magenta', 'orange', 'pink', 'red', 'salmon', 'turquoise', 'vermilion', 'white', 'yellow']
    makes=['Audi', 'Bentley', 'DaimlerChrysler', 'Ferrari', 'GM', 'Honda', 'Jaguar', 'KIA', 'Lamborghini', 'Maserati', 'Nissan', 'Porsche', 'Rolls-Royce', 'Suzuki', 'Toyota', 'Volvo']
    pins = PIN.loadPINs(lAffinity.qProto("SELECT * FROM workers;"))
    statements = []
    for i in range(len(pins)):
        statements.append("UPDATE " + str(pins[pickFrom(len(pins))].mPID) + " ADD cars=(INSERT car_make=\'" + makes[pickFrom(len(makes))] + "\', color=\'" + colors[pickFrom(len(colors))] + "\', year=\'" + str(1950 + int(math.floor(random.random() * 62))) + "\');")
    lAffinity.startTx()
    for stmt in statements:
        lAffinity.q(stmt)
    lAffinity.commitTx()
    lAffinity.q("CREATE CLASS cars AS SELECT * WHERE EXISTS(car_make);")
    print "Step9 complete!" 

    # step10
    # friends = q("SELECT * FROM workers.friends;");
    # for (i = 0; i < friends.length; i++) print("friend of a worker: " + friends[i].name + " (" + friends[i].id + ")");
    # q("SELECT * FROM workers('mathematician').friends.friends.friends;");
    # q("SELECT name, profession FROM workers(@['d', 'n']).friends[age > 10 AND age < 50].friends;");
    # q("SELECT * FROM people(@['A', 'J']).friends[SUBSTR(name, 0, 1) IN @['A', 'J']];");
    # q("SELECT * FROM workers(@['d', 'n']).friends{+}[age < 50];");
    # q("SELECT * FROM workers(@['d', 'n']).friends{*}.cars[year IN @[1980, 2000]];");
    print "Start step10"
    friends = PIN.loadPINs(lAffinity.qProto("SELECT * FROM workers.friends;"))
    for i in range(len(friends)):
        print "friend of a worker: " + friends[i]['name'] + " (" + str(friends[i].mPID) + ")"
    # TODO: How to verify the results?
    lAffinity.q("SELECT * FROM workers('mathematician').friends.friends.friends;")
    lAffinity.q("SELECT name, profession FROM workers(@['d', 'n']).friends[age > 10 AND age < 50].friends;")
    lAffinity.q("SELECT * FROM people(@['A', 'J']).friends[SUBSTR(name, 0, 1) IN @['A', 'J']];")
    lAffinity.q("SELECT * FROM workers(@['d', 'n']).friends{+}[age < 50];")
    lAffinity.q("SELECT * FROM workers(@['d', 'n']).friends{*}.cars[year IN @[1980, 2000]];")
    print "Step10 complete!" 
    
    # step11
    # q("SELECT * FROM cars AS c JOIN workers(@['d', 'n']) AS w ON (c.afy:pinID = w.cars) WHERE (c.year IN @[1980, 2000]);");
    # print "JSON result: %s" % lAffinity.check("SELECT * FROM cars AS c JOIN workers(@['d', 'n']) AS w ON (c.afy:pinID = w.cars) WHERE (c.year IN @[1980, 2000]);")
    print "Start step11"
    pins = PIN.loadPINs(lAffinity.qProto("SELECT * FROM cars AS c JOIN workers(@['d', 'n']) AS w ON (c.afy:pinID = w.cars) WHERE (c.year IN @[1980, 2000]);"))
    assert len(pins) > 0
    print "Step11 complete!" 

    # for repro bug#302
    lAffinity.q("DELETE FROM *;")
    assert lAffinity.qCount("select * where not exists(afy:predicate)") == 0
    lAffinity.close()	
    print "Test tutorial complete!"
    return True

def _entryPoint():
    start = time.time()
    test_executable_doc() # TODO: Review... doesn't seem happy...
    test_tutorial()
    end = time.time()
    print "TestOnlineMaterials costs : " + timeToString(end - start)
    
class TestOnlineMaterials(AffinityTest):
    def execute(self):
        _entryPoint()

AffinityTest.declare(TestOnlineMaterials)

if __name__ == '__main__':
    lT = TestOnlineMaterials()
    lT.execute()

