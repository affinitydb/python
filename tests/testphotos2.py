#!/usr/bin/env python2.6
"""This module is a small photo sharing application, written exclusively with 'pathSQL in' (no 'protobuf in').
The present version uses collections of references and path expressions to model the principal relationships
between objects, whereas 'testphotos1.py', its counterpart, implements the same application with
a relational model with joins."""
from testfwk import AffinityTest
from affinity import *
from testphotos_common import *
import random
import string
import uuid
import os
import time
import datetime
import modeling

lAffinity = AFFINITY()
lInMemoryChk = InMemoryChk()
def _describeModel():
    "Use the modeling extension to describe our schema."
    # Review: Should probably use the singular convention for entities... in all samples... also coherent with "is a" etc... identify if there are inconsistencies...
    modeling.initialize()
    modeling.describeClass("testphotos2_class_photos", {modeling.AFYURI_PROP_DOCSTRING:"General category for photos, by id. Photos typically also contain a 'tags' property."})
    modeling.describeClass("testphotos2_class_tags", {modeling.AFYURI_PROP_DOCSTRING:"General category for unique tags, which logically group photos together into sets, and serve as the basic unit to grant privileges to users and groups."})
    modeling.describeClass("testphotos2_class_groups", {modeling.AFYURI_PROP_DOCSTRING:"Category for groups of users. Groups typically also contain a 'tags' property, representing the sets of photos to which a group was granted access."})
    modeling.describeClass("testphotos2_class_users", {modeling.AFYURI_PROP_DOCSTRING:"Category for users (guests of an instance of the photo system). Users typically also contain a 'tags' property, representing the sets of photos to which an individual user was granted access (additionally to the access rights of the group it this user belongs to)."})
    modeling.describeRelation("testphotos2_tags", {modeling.AFYURI_PROP_DOCSTRING:"Collection of tags.", modeling.AFYURI_PROP_CATEGORY:"testphotos2_class_tags"})
    modeling.describeRelation("testphotos2_tags", {modeling.AFYURI_PROP_DOCSTRING:"All photos containing the same tag form a set (album).", modeling.AFYURI_PROP_CARDINALITY:modeling.AFY_CARDINALITY_ONE_TO_MANY}, pClassURI="testphotos2_class_photos")
    modeling.describeRelation("testphotos2_tags", {modeling.AFYURI_PROP_DOCSTRING:"All users of a group are granted permission to view the photos tagged with the tags of their group.", modeling.AFYURI_PROP_CARDINALITY:modeling.AFY_CARDINALITY_ONE_TO_MANY}, pClassURI="testphotos2_class_groups")
    modeling.describeRelation("testphotos2_tags", {modeling.AFYURI_PROP_DOCSTRING:"A user is granted permission to view the photos tagged with its tags.", modeling.AFYURI_PROP_CARDINALITY:modeling.AFY_CARDINALITY_ONE_TO_MANY}, pClassURI="testphotos2_class_users")
    modeling.describeRelation("testphotos2_users", {modeling.AFYURI_PROP_DOCSTRING:"Collection of users.", modeling.AFYURI_PROP_CATEGORY:"testphotos2_class_users"})
    modeling.describeRelation("testphotos2_users", {modeling.AFYURI_PROP_DOCSTRING:"A group is composed of users.", modeling.AFYURI_PROP_CARDINALITY:modeling.AFY_CARDINALITY_ONE_TO_MANY}, pClassURI="testphotos2_class_groups")
    modeling.describeAttribute("testphotos2_id", {modeling.AFYURI_PROP_DOCSTRING:"The unique id of a photo is built from the sha-1 digest of the pixels (n.b. in this demo, it's just a random guid)."}, pClassURI="testphotos2_class_photos")
    modeling.describeAttribute("testphotos2_id", {modeling.AFYURI_PROP_DOCSTRING:"The unique id of a user is an email id."}, pClassURI="testphotos2_class_users")
    modeling.describeAttribute("testphotos2_id", {modeling.AFYURI_PROP_DOCSTRING:"The unique id of a group is the name of the group."}, pClassURI="testphotos2_class_groups")
    modeling.describeAttribute("testphotos2_tag", {modeling.AFYURI_PROP_DOCSTRING:"A tag is a text string representative of a collection of photos."}, pClassURI="testphotos2_class_tags")
    modeling.describeAttribute("testphotos2_pw", {modeling.AFYURI_PROP_DOCSTRING:"pw is a guest user's individual password, securing his access to the database of photos."}, pClassURI="testphotos2_class_users")
    modeling.describeAttribute("testphotos2_date", {modeling.AFYURI_PROP_DOCSTRING:"The date the picture was taken. Used to order the presentation of photos in a tag, and to provide date-driven views on photos accessible to a user."}, pClassURI="testphotos2_class_photos")
    modeling.describeAttribute("testphotos2_time", {modeling.AFYURI_PROP_DOCSTRING:"The time of day the picture was taken. Used to order the presentation of photos in a tag."}, pClassURI="testphotos2_class_photos")
    modeling.describeAttribute("testphotos2_path", {modeling.AFYURI_PROP_DOCSTRING:"The physical file path of the photo."}, pClassURI="testphotos2_class_photos")
    modeling.describeAttribute("testphotos2_name", {modeling.AFYURI_PROP_DOCSTRING:"The file name of the photo."}, pClassURI="testphotos2_class_photos")
def _getpins(_pQuery):
    "Query and return array of pins."
    return PIN.loadPINs(lAffinity.qProto(_pQuery))
def _chkCount(_pName, _pExpected, _pActual):
    "Print expected vs actual counts; pause for a second if the result is not correct."
    print ("%s: expected %s %s, found %s." % (("WARNING", "YEAH")[_pExpected == _pActual], _pExpected, _pName, _pActual))
    assert _pExpected == _pActual
def _onWalk(_pArg, _pDir, _pFileNames):
    "Traverse a directory structure and call pArg[1] on every file matching extension pArg[0]; stop after pArg[3] > pArg[2], unless they're null."
    for _f in _pFileNames:
        if (len(_pArg) > 3) and (_pArg[3] != None) and _pArg[2] and (_pArg[3] > _pArg[2]):
            return
        elif _f.rfind(".%s" % _pArg[0]) == len(_f) - 1 - len(_pArg[0]):
            if (len(_pArg) > 3) and (_pArg[3] != None):
                _pArg[3] = _pArg[3] + 1
            _pArg[1](_pDir, _f)
        elif os.path.isdir(_f):
            os.path.walk(_f, _onWalk, _pArg)
def _createPhoto(_pDir, _pFileName):
    "Register a photo object in the photos table."
    print ("adding file %s/%s" % (_pDir, _pFileName))
    _lFullPath = "%s/%s" % (_pDir, _pFileName)
    _lDate = datetime.datetime.fromtimestamp(os.path.getctime(_lFullPath))
    lAffinity.q("INSERT (testphotos2_id, testphotos2_date, testphotos2_time, testphotos2_path, testphotos2_name, testphotos2_fullname) VALUES ('%s', TIMESTAMP'%s', INTERVAL'%s', '%s', '%s', '%s');" % \
        (uuid.uuid4().hex, AffinityTest.strftime(_lDate, "%4Y-%2m-%2d"), AffinityTest.strftime(_lDate, "%2H:%2M:%2S"), _pDir, _pFileName, _lFullPath))
def _randomTag(_pTagName, _pRatio=0.10):
    "Assign pTagName to a random selection of 'photos'."
    # Make sure the tag is registered in the 'tags' table.
    lAffinity.q("START TRANSACTION;")
    _lTags = _getpins("SELECT * FROM testphotos2_class_tags('%s');" % _pTagName)
    if 0 == len(_lTags):
        print ("adding tag %s" % _pTagName)
        _lTags = PIN.loadPINs(lAffinity.qProto("INSERT (testphotos2_tag) VALUES ('%s');" % _pTagName))
    # Select an arbitrary number of 'photos', and tag them.
    for _iP in _getpins("SELECT * FROM testphotos2_class_photos;"):
        if random.random() <= _pRatio:
            lAffinity.q("UPDATE %s ADD testphotos2_tags=%s;" % (_iP.mPID, _lTags[0].mPID))
            lInMemoryChk.tagPhoto(_iP["testphotos2_id"], _pTagName)
    lAffinity.q("COMMIT;")
def _randomGroupPrivileges():
    "Assign a random selection of tags to each existing group."
    # Get the existing groups.
    _lGroups = _getpins("SELECT * FROM testphotos2_class_groups;")
    print ("groups: %s" % [_ip["testphotos2_id"] for _ip in _lGroups])
    # Get the existing tags.
    _lTags = _getpins("SELECT * FROM testphotos2_class_tags;")
    print ("tags: %s" % [_ip["testphotos2_tag"] for _ip in _lTags])
    # Attribute randomly the privilege to see some tags to each group.
    for _iG in _lGroups:
        _lRights = random.sample(_lTags, random.randrange(len(_lTags) / 2))
        for _iR in _lRights:
            lAffinity.q("UPDATE %s ADD testphotos2_tags=%s;" % (repr(_iG.mPID), repr(_iR.mPID)))
            lInMemoryChk.addGroupPrivilege(_iG["testphotos2_id"], _iR["testphotos2_tag"])
def _randomUserPrivileges():
    "Assign a random selection of tags to each existing user."
    # Get the existing users.
    _lUsers = _getpins("SELECT * FROM testphotos2_class_users;")
    print ("users: %s" % [_ip["testphotos2_id"] for _ip in _lUsers])
    # Get the existing tags.
    _lTags = _getpins("SELECT * FROM testphotos2_class_tags;")
    print ("tags: %s" % [_ip["testphotos2_tag"] for _ip in _lTags])
    # Attribute randomly the privilege to see some tags to each user.
    for _iU in _lUsers:
        _lRights = random.sample(_lTags, random.randrange(len(_lTags)))
        for _iR in _lRights:
            lAffinity.q("UPDATE %s ADD testphotos2_tags=%s;" % (repr(_iU.mPID), repr(_iR.mPID)))
            lInMemoryChk.addUserPrivilege(_iU["testphotos2_id"], _iR["testphotos2_tag"])
def _entryPoint():
    # Start.
    lAffinity.open()
    # Self-document.
    _describeModel()
    # Create a few classes.
    print ("Creating classes.")
    try:
        lAffinity.q("CREATE CLASS testphotos2_class_photos AS SELECT * WHERE testphotos2_id IN :0 AND EXISTS(testphotos2_date) AND EXISTS(testphotos2_time) AND EXISTS(testphotos2_path) AND EXISTS (testphotos2_name) AND EXISTS(testphotos2_fullname);") # Note: The pins that conform with this also have a 'tags' field (a collection of references to tags on the photo).
        lAffinity.q("CREATE CLASS testphotos2_class_tags AS SELECT * WHERE testphotos2_tag in :0 AND NOT EXISTS(testphotos2_id);") # Interesting... without "AND NOT EXISTS...", it indexes everything... which could be cool, if only I could listValues to retrieve my distinct tags...
        lAffinity.q("CREATE CLASS testphotos2_class_groups AS SELECT * WHERE testphotos2_id in :0 AND EXISTS(testphotos2_users);") # Note: The pins that conform with this also have a 'tags' field (a collection of references to tags to which the group was granted access).
        lAffinity.q("CREATE CLASS testphotos2_class_users AS SELECT * WHERE testphotos2_id in :0 AND EXISTS(testphotos2_pw);") # Note: The pins that conform with this also have a 'tags' field (a collection of references to tags to which the user was granted access).
    except:
        pass
    # Delete old instances, if any.
    print ("Deleting old data.")
    lAffinity.q("DELETE FROM testphotos2_class_photos;")
    lAffinity.q("DELETE FROM testphotos2_class_tags;")
    lAffinity.q("DELETE FROM testphotos2_class_groups;")
    lAffinity.q("DELETE FROM testphotos2_class_users;")
    # Create a few photos.
    lAffinity.q("START TRANSACTION;")
    POPULATE_WALK_THIS_DIRECTORY="../tests_kernel"
    POPULATE_USE_THIS_EXTENSION="cpp"
    lCreateWalkArgs = [POPULATE_USE_THIS_EXTENSION, _createPhoto, None, 0]
    os.path.walk(POPULATE_WALK_THIS_DIRECTORY, _onWalk, lCreateWalkArgs)
    lAffinity.q("COMMIT;")
    lCntPhotos = lAffinity.qCount("SELECT * FROM testphotos2_class_photos;")
    _chkCount("photos", _pExpected=lCreateWalkArgs[3], _pActual=lCntPhotos)
    # Create a few tags and tag some photos.
    lAffinity.q("START TRANSACTION;")
    lSomeTags = ("cousin_vinny", "uncle_buck", "sister_suffragette", "country", "city", "zoo", "mountain_2010", "ocean_2004", "Beijing_1999", "Montreal_2003", "LasVegas_2007", "Fred", "Alice", "sceneries", "artwork")
    for iT in lSomeTags:
        _randomTag(iT)
    lAffinity.q("COMMIT;")
    # Create a few users and groups.
    lAffinity.q("START TRANSACTION;")
    lGroups = ("friends", "family", "public")
    lUsers = ("ralph@peanut.com", "stephen@banana.com", "wilhelm@orchestra.com", "sita@marvel.com", "anna@karenina.com", "leo@tolstoy.com", "peter@pan.com", "jack@jill.com", "little@big.com", \
        "john@hurray.com", "claire@obscure.com", "stanley@puck.com", "grey@ball.com", "john@wimbledon.com", "mark@up.com", "sabrina@cool.com")
    for iU in lUsers:
        lGroup = random.choice(lGroups)
        lNewUserPin = _getpins("INSERT (testphotos2_id, testphotos2_pw) VALUES ('%s', '%s');" % (iU, ''.join(random.choice(string.letters) for i in xrange(20))))
        lGroupPin = _getpins("SELECT * FROM testphotos2_class_groups('%s');" % lGroup) # Convenient elision: pass x, it evaluates [x, x].
        lInMemoryChk.setUserGroup(iU, lGroup)
        if 0 == len(lGroupPin):
            lAffinity.q("INSERT (testphotos2_id, testphotos2_users) VALUES ('%s', %s);" % (lGroup, lNewUserPin[0].mPID))
        else:
            lAffinity.q("UPDATE %s ADD testphotos2_users=%s;" % (lGroupPin[0].mPID, lNewUserPin[0].mPID))
            lCntUsersInGroup = lAffinity.qCount("SELECT * FROM %s.testphotos2_users;" % repr(lGroupPin[0].mPID))
            print ("group %s contains %d users" % (lGroup, lCntUsersInGroup))
    lAffinity.q("COMMIT;")
    lCntUserGroups = lAffinity.qCount("SELECT * FROM testphotos2_class_groups;")
    lCntUsers = lAffinity.qCount("SELECT * FROM testphotos2_class_users;")
    _chkCount("groups", _pExpected=len(lGroups), _pActual=lCntUserGroups)
    _chkCount("users", _pExpected=len(lUsers), _pActual=lCntUsers)
    # Assign group/user privileges, and query on those.
    _randomGroupPrivileges()
    _randomUserPrivileges()
    # Find a user who can view any of the first 5 tags, and count how many photos he can view.
    lTags = _getpins("SELECT * FROM testphotos2_class_tags;")
    lFirstTagsStr = ','.join(["'%s'" % iT["testphotos2_tag"] for iT in lTags[:5]])
    #lUsersOfInterest = _getpins("SELECT * FROM testphotos2_class_users WHERE testphotos2_tags.testphotos2_tag IN (%s);" % lFirstTagsStr)
    #lUsersOfInterest = _getpins("SELECT u.* FROM testphotos2_class_tags(%s) AS t JOIN testphotos2_class_users AS u ON (t.afy:pinID = u.testphotos2_tags);" % lFirstTagsStr) # doesn't work yet
    lUsersOfInterest = _getpins("SELECT * FROM testphotos2_class_users AS u JOIN testphotos2_class_tags(%s) AS t ON (t.afy:pinID = u.testphotos2_tags);" % lFirstTagsStr)
    print ("users that have one of %s: %s" % (lFirstTagsStr, [iU.get("testphotos2_id") for iU in lUsersOfInterest]))
    def _countUserPhotos(_pUserName, _pUserGroup):
        _lTagsP1 = _getpins("SELECT * FROM testphotos2_class_tags AS t JOIN testphotos2_class_users('%s') AS u ON (t.afy:pinID = u.testphotos2_tags);" % _pUserName)
        _lExpected_usPriv = lInMemoryChk.getTags_usPriv(_pUserName)
        _lTags = set() # Note: Here it's not for dedup, just for compatibility with InMemoryChk...
        for _iP in _lTagsP1:
            _lTags.add(_iP["testphotos2_tag"])
        print ("user %s has user privilege tags %s" % (_pUserName, _lTags))
        if len(_lExpected_usPriv.difference(_lTags)) > 0:
            print ("WARNING: expected user-privilege tags %s" % _lExpected_usPriv.difference(_lTags))
            assert False
        _lTagsP2 = _getpins("SELECT * FROM testphotos2_class_tags AS t JOIN testphotos2_class_groups('%s') AS g ON (t.afy:pinID = g.testphotos2_tags);" % _pUserGroup)
        for _iP in _lTagsP2:
            _lTags.add(_iP["testphotos2_tag"])
        print ("user %s has tags %s" % (_pUserName, _lTags))
        _lExpectedTags = lInMemoryChk.getUserTags(_pUserName)
        if len(_lExpectedTags.difference(_lTags)) > 0:
            print ("WARNING: expected tags %s" % _lExpectedTags.difference(_lTags))
            assert False
        _lTags = _lTagsP1
        _lTags.extend(_lTagsP2)
        _lPhotos = _getpins("SELECT * FROM testphotos2_class_photos WHERE testphotos2_tags IN (%s);" % ','.join([repr(_iT.mPID) for _iT in _lTags]))
        print "found %d photos" % len(_lPhotos)
        _lUniquePhotos = set()
        for _iP in _lPhotos:
            _lUniquePhotos.add(_iP["testphotos2_id"])
        if len(_lPhotos) != len(_lUniquePhotos):
            print ("non-unique: %s unique: %s" % (len(_lPhotos), len(_lUniquePhotos)))
        return len(_lUniquePhotos)
    for iU in lUsersOfInterest:
        lGroupName = _getpins("SELECT * FROM testphotos2_class_groups WHERE (%s = testphotos2_users);" % iU.mPID)[0]["testphotos2_id"]
        lCntPhotos = _countUserPhotos(iU["testphotos2_id"], lGroupName)
        _chkCount("photos that can be viewed by %s" % iU["testphotos2_id"], _pExpected=lInMemoryChk.countPhotos(iU["testphotos2_id"]), _pActual=lCntPhotos)
    if False:
        # Output the final state.
        print (_getpins("SELECT *;"))
    # Close everything.
    lAffinity.close()

class TestPhotos2(AffinityTest):
    "A simple application talking to the store through pathSQL only, and using a model with collections of references."
    def execute(self):
        _entryPoint()
AffinityTest.declare(TestPhotos2)

if __name__ == '__main__':
    lT = TestPhotos2()
    lT.execute()
