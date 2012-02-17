#!/usr/bin/env python2.6
"""This module demonstrates basic, typical interactions with the python client library.
It's meant as an introductory material, to complement the documentation.
It should be less cluttered by validation code than the tests, and more readable."""
from affinity import *

if __name__ == '__main__':
    lAffinity = AFFINITY()
    lAffinity.open()

    print ("\n1. Create a simple PIN via a direct pathSQL request to the Affinity server.")
    lAffinity.q("INSERT (name, functions) VALUES ('Peter', 'dentist');")

    print ("\n2. Create a simple PIN via a pathSQL request embedded inside a protobuf message sent to the Affinity server.")
    lAffinity.qProto("INSERT (name, functions) VALUES ('Fred', 'engineer');")

    print ("\n3. Same as 2, but grabbing the resulting PIN for further manipulation.")
    lPinAnn = PIN.loadPINs(lAffinity.qProto("INSERT (name, functions) VALUES ('Ann', 'scientist');"))[0]
    print ("is %s really Ann?" % lPinAnn["name"])
    print ("is %s really a %s?" % (lPinAnn["name"], lPinAnn["functions"]))

    print ("\n4. Add another function to Ann, via pathSQL: mother.")
    lAffinity.qProto("UPDATE %s ADD functions='mother';" % lPinAnn.mPID)
    lPinAnn.refreshPIN()

    print ("\n5. Add a few other functions to Ann, via a protobuf message; insert them between 'scientist' and 'mother'.")
    lEidPivot = lPinAnn.getExtra("functions", pEpos=1).mEid
    lOtherAnnFunctions = ("swimmer", "chef", "decorator", "advisor")
    for iF in lOtherAnnFunctions:
        PIN({PIN.SK_PID:lPinAnn.mPID, "functions":(iF, PIN.Extra(pOp=affinity_pb2.Value.OP_ADD_BEFORE, pEid=lEidPivot))}).savePIN()
    lPinAnn.refreshPIN() # Note: This refresh won't be needed when bug #171 is fixed.
    print (lPinAnn)

    print ("\n6. Relieve Ann of a few functions: %s" % lPinAnn["functions"][2:4])
    del lPinAnn["functions"][2:4]
    print (lPinAnn)

    print ("\n7. Create more PINs via a pure protobuf message sent to the server.")
    lNewPins = PIN.savePINs([
        PIN({"name":"Sabrina", "functions":"dancer", "age":32}), 
        PIN({"name":"Sophia", "functions":["artist", "scientist"], "age":99}),
        PIN({"name":"Allan", "age":45})])
    print ("New people: %s" % [iP["name"] for iP in lNewPins])

    print ("\n8. Sort Ann's functions alphabetically, by just moving elements around.")
    lPinAnn["functions"].sort()
    print (lPinAnn["functions"])

    print ("\n9. Specify Ann's age, increment Allan's, overwrite Sabrina's, and delete Sophia's.")
    lPins = (lPinAnn, lNewPins[2], lNewPins[0], lNewPins[1])
    print ("ages before: %s" % ''.join("%s=%s " % (i.get("name"), i.get("age")) for i in lPins))
    lPinAnn["age"] = 36.5
    PIN({"age":(1.5, PIN.Extra(pOp=affinity_pb2.Value.OP_PLUS))}, __PID__=lNewPins[2].mPID).savePIN(); lNewPins[2].refreshPIN()
    lNewPins[0]["age"] = 37
    del lNewPins[1]["age"]
    print ("ages after: %s" % ''.join("%s=%s " % (i.get("name"), i.get("age")) for i in lPins))

    print ("\n10. Create a family for people of known age; query using this family, and order by age.")
    try:
        # Note:
        #   Checking for class existence is not mandatory, provided that class creation is done within try-except;
        #   here, for documentation sake, both approaches are demonstrated in combination.  
        if 0 == lAffinity.qCount("SELECT * FROM afy:ClassOfClasses WHERE afy:classID='sample1_knownage';"):
            lAffinity.qProto("CREATE CLASS sample1_knownage AS SELECT * WHERE age IN :0 and EXISTS(name);")
        else:
            print ("family already existed.")
    except:
        pass
    print (''.join("%s=%s " % (i.get("name"), i.get("age")) for i in PIN.loadPINs(lAffinity.qProto("SELECT * FROM sample1_knownage ORDER BY age;"))))

    # TODO: show more kinds of modifs (pathSQL and direct)
    # TODO: show more queries
    # TODO: really show transactions

    print ("\nFINAL. Print the resulting PINs.")
    print (PIN.loadPINs(lAffinity.qProto("SELECT * WHERE EXISTS(name);")))
    lAffinity.close()
