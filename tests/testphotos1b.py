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
"""This is the same thing as testphotos1, but using qnames.
In server mode, this requires a keep-alive connection."""
from testfwk import AffinityTest
from affinity import *
from testphotos_common import *
import random
import string
import uuid
import os
import time
import datetime

if not sys.modules.has_key('affinityinproc') or not AffinityConnection.DEFAULT_INPROC:
    print ("Running in server mode - relying on keep-alive.");

lAffinity = AFFINITY()
# Setting up prefixes for qnames
lAffinity.setPrefix("testphotos1","http://localhost/afy/class/testphotos1b/")
lAffinity.setPrefix("tagont","http://code.google.com/p/tagont/")
lAffinity.setPrefix("foaf", "http://xmlns.com/foaf/0.1/")
lAffinity.setPrefix("semanticdesktop", "http://www.semanticdesktop.org/ontologies/2007/03/22/")
lInMemoryChk = InMemoryChk()
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
def _selectDistinctGroups():
    # Review: eventually Affinity will allow to SELECT DISTINCT(groupid) FROM users...
    _lGroupsRaw = _getpins("SELECT * FROM testphotos1:user;")
    _lGroupsIds = set()
    for _iG in _lGroupsRaw:
        _lGroupsIds.add(_iG["http://xmlns.com/foaf/0.1/member/adomain:Group"])
    return _lGroupsIds
def _createPhoto(_pDir, _pFileName):
    "Register a photo object in the photos table."
    print ("adding file %s/%s" % (_pDir, _pFileName))
    _lFullPath = "%s/%s" % (_pDir, _pFileName)
    _lDate = datetime.datetime.fromtimestamp(os.path.getctime(_lFullPath))
    lAffinity.q("INSERT (semanticdesktop:\"nfo#hasHash\", \"http://www.w3.org/2001/XMLSchema#date\", \"http://www.w3.org/2001/XMLSchema#time\", semanticdesktop:\"nfo#fileUrl\", semanticdesktop:\"nfo#fileName\") VALUES ('%s', TIMESTAMP'%s', INTERVAL'%s', '%s', '%s');" % \
        (uuid.uuid4().hex, AffinityTest.strftime(_lDate, "%4Y-%2m-%2d"), AffinityTest.strftime(_lDate, "%2H:%2M:%2S"), _pDir, _pFileName))
def _randomTag(_pTagName, _pRatio=0.10):
    "Assign pTagName to a random selection of 'photos'."
    # Make sure the tag is registered in the 'tags' table.
    # Note: Using UNIQUE to handle duplicates (e.g. http://www.tutorialspoint.com/mysql/mysql-handling-duplicates.htm)
    #       is not an option with pathSQL...
    lAffinity.q("START TRANSACTION;")
    _lCount = lAffinity.qCount("SELECT * FROM testphotos1:tag WHERE tagont:hasTagLabel='%s';" % _pTagName) # review: index by tag
    if 0 == _lCount:
        print ("adding tag %s" % _pTagName)
        lAffinity.q("INSERT (tagont:hasTagLabel) VALUES ('%s');" % _pTagName)
    # Select an arbitrary number of 'photos', and tag them.
    for _iP in _getpins("SELECT * FROM testphotos1:photo;"):
        if random.random() <= _pRatio:
            lAffinity.q("INSERT (semanticdesktop:\"nfo#hasHash\", tagont:hasTagLabel) VALUES ('%s', '%s');" % (_iP["http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#hasHash"], _pTagName))
            lInMemoryChk.tagPhoto(_iP["http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#hasHash"], _pTagName)
    lAffinity.q("COMMIT;")
def _randomGroupPrivileges():
    "Assign a random selection of tags to each existing group."
    # Get the existing groups.
    _lGroupIds = _selectDistinctGroups()
    print ("groups: %s" % _lGroupIds)
    # Get the existing tags.
    _lTags = _getpins("SELECT * FROM testphotos1:tag;")
    print ("tags: %s" % [_ip["http://code.google.com/p/tagont/hasTagLabel"] for _ip in _lTags])
    # Attribute randomly the privilege to see some tags to each group.
    for _iG in _lGroupIds:
        _lRights = random.sample(_lTags, random.randrange(len(_lTags) / 2))
        for _iR in _lRights:
            lAffinity.q("INSERT (tagont:hasTagLabel, tagont:hasVisibility) VALUES ('%s', '%s');" % (_iR["http://code.google.com/p/tagont/hasTagLabel"], _iG))
            lInMemoryChk.addGroupPrivilege(_iG, _iR["http://code.google.com/p/tagont/hasTagLabel"])
def _randomUserPrivileges():
    "Assign a random selection of tags to each existing user."
    # Get the existing users.
    _lUsers = _getpins("SELECT * FROM testphotos1:user;")
    print ("users: %s" % [_ip["http://xmlns.com/foaf/0.1/mbox"] for _ip in _lUsers])
    # Get the existing tags.
    _lTags = _getpins("SELECT * FROM testphotos1:tag;")
    print ("tags: %s" % [_ip["http://code.google.com/p/tagont/hasTagLabel"] for _ip in _lTags])
    # Attribute randomly the privilege to see some tags to each user.
    for _iU in _lUsers:
        _lRights = random.sample(_lTags, random.randrange(len(_lTags)))
        for _iR in _lRights:
            lAffinity.q("INSERT (tagont:hasTagLabel, tagont:hasVisibility) VALUES ('%s', '%s');" % (_iR["http://code.google.com/p/tagont/hasTagLabel"], _iU["http://xmlns.com/foaf/0.1/mbox"]))
            lInMemoryChk.addUserPrivilege(_iU["http://xmlns.com/foaf/0.1/mbox"], _iR["http://code.google.com/p/tagont/hasTagLabel"])
def _entryPoint():
    # Start.
    lAffinity.open(pKeepAlive=True)
    # Create a few classes.
    # Note:
    #   The naming convention adopted here aims at illustrating how a consistent naming scheme borrowed from
    #   existing ontologies can be reused in Affinity, to lay a data model on (hopefully) stable ground.
    #   The goal here is not to publish our data on the semantic web (although nothing prevents someone to pursue that goal).
    # Note:
    #   Here, we take some liberty with the strict interpretation of some of the URIs. For example, in the original
    #   ontology, 'hasVisibility' is not defined with a range of groups and people, but rather with an enumeration
    #   (private/public/protected).
    # Note:
    #   We follow the following simple guidelines:
    #   . we interpret Affinity classification as a separate phase of data modeling, targeted for
    #     application-logic structure rather than a fundamental characteristic of the data itself;
    #     hence, all our class names here are "local"
    #   . we only reuse _property_ URIs from existing ontologies; in some cases, we may concatenate property URIs with
    #     class URIs, when the property URIs are insufficient to implicitly delimit their own attribute domain;
    #     we use "/adomain:" to operate this concatenation
    # Note:
    #   The term "attribute domain" is taken from relational modeling, and designates the domain from which
    #   values of the attribute can be taken (usual mathematical meaning). The rdfs/owl terminology
    #   assigns a different meaning to "rdfs:domain"... "rdfs:range" is closer to our "adomain".
    print ("Creating classes.")
    try:
        lAffinity.q("CREATE CLASS \"http://localhost/afy/class/testphotos1b/photo\" AS SELECT * WHERE semanticdesktop:\"nfo#hasHash\" IN :0 AND EXISTS(\"http://www.w3.org/2001/XMLSchema#date\") AND EXISTS(\"http://www.w3.org/2001/XMLSchema#time\") AND EXISTS(semanticdesktop:\"nfo#fileUrl\") AND EXISTS (semanticdesktop:\"nfo#fileName\");")
        lAffinity.q("CREATE CLASS testphotos1:tag AS SELECT * WHERE tagont:hasTagLabel in :0 AND NOT EXISTS(semanticdesktop:\"nfo#hasHash\") AND NOT EXISTS(tagont:hasVisibility);") # Interesting... without "AND NOT EXISTS(id)", it indexes also my tagging table and my privilege table... which would be cool, if only I could listValues to retrieve my distinct tags...
        lAffinity.q("CREATE CLASS testphotos1:tagging AS SELECT * WHERE EXISTS(tagont:hasTagLabel) AND semanticdesktop:\"nfo#hasHash\" in :0;")
        lAffinity.q("CREATE CLASS testphotos1:user AS SELECT * WHERE foaf:mbox in :0 AND EXISTS(\"http://www.w3.org/2002/01/p3prdfv1#user.login.password\") AND EXISTS(foaf:\"member/adomain:Group\");")
        lAffinity.q("CREATE CLASS testphotos1:privilege AS SELECT * WHERE tagont:hasTagLabel in :0 and EXISTS(tagont:hasVisibility);")
    except:
        pass
    # Delete old instances, if any.
    print ("Deleting old data.")
    lAffinity.q("DELETE FROM testphotos1:photo;")
    lAffinity.q("DELETE FROM testphotos1:tag;")
    lAffinity.q("DELETE FROM testphotos1:tagging;")
    lAffinity.q("DELETE FROM testphotos1:user;")
    lAffinity.q("DELETE FROM testphotos1:privilege;")
    # lAffinity.q("DELETE FROM \"http://localhost/afy/class/testphotos1b/privilege\";") # TODO: review why this fails after running testphotos1...
    # Create a few photos.
    lAffinity.q("START TRANSACTION;")
    POPULATE_WALK_THIS_DIRECTORY="../tests_kernel"
    POPULATE_USE_THIS_EXTENSION="cpp"
    lCreateWalkArgs = [POPULATE_USE_THIS_EXTENSION, _createPhoto, None, 0]
    os.path.walk(POPULATE_WALK_THIS_DIRECTORY, _onWalk, lCreateWalkArgs)
    lAffinity.q("COMMIT;")
    lCntPhotos = lAffinity.qCount("SELECT * FROM testphotos1:photo;")
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
    #lGroups = ("friends", "family")
    lUsers = ("ralph@peanut.com", "stephen@banana.com", "wilhelm@orchestra.com", "sita@marvel.com", "anna@karenina.com", "leo@tolstoy.com", "peter@pan.com", "jack@jill.com", "little@big.com", \
        "john@hurray.com", "claire@obscure.com", "stanley@puck.com", "grey@ball.com", "john@wimbledon.com", "mark@up.com", "sabrina@cool.com")
    for iU in lUsers:
        lGroup = random.choice(lGroups)
        lNewUserPin = _getpins("INSERT (foaf:mbox, \"http://www.w3.org/2002/01/p3prdfv1#user.login.password\", foaf:\"member/adomain:Group\") VALUES ('%s', '%s', '%s');" % (iU, ''.join(random.choice(string.letters) for i in xrange(20)), lGroup))
        lInMemoryChk.setUserGroup(iU, lGroup)
        lCntUsersInGroup = lAffinity.qCount("SELECT * FROM testphotos1:user WHERE foaf:\"member/adomain:Group\"='%s';" % lGroup)
        print ("group %s contains %d users" % (lGroup, lCntUsersInGroup))
    lAffinity.q("COMMIT;")

    lCntUserGroups = len(_selectDistinctGroups())
    lCntUsers = lAffinity.qCount("SELECT * FROM testphotos1:user;")
    _chkCount("groups", _pExpected=len(lGroups), _pActual=lCntUserGroups)
    _chkCount("users", _pExpected=len(lUsers), _pActual=lCntUsers)
    # Assign group/user privileges, and query on those.
    _randomGroupPrivileges()
    _randomUserPrivileges()
    lCntPrivileges = lAffinity.qCount("SELECT * FROM testphotos1:privilege;")
    print ("%d privileges assigned." % lCntPrivileges)
    # Find a user who can view any of the first 5 tags, and count how many photos he can view.
    lTags = _getpins("SELECT * FROM testphotos1:tag;")
    lFirstTagStr = "'%s'" % lTags[0]["http://code.google.com/p/tagont/hasTagLabel"]
    lFirstTagsStr = ','.join(["'%s'" % iT["http://code.google.com/p/tagont/hasTagLabel"] for iT in lTags[:5]])
    lUsersOfInterest = _getpins("SELECT * FROM testphotos1:privilege(%s) AS p JOIN testphotos1:user AS u ON (p.tagont:hasVisibility = u.foaf:mbox);" % lFirstTagStr)
    # TODO: isolate and log as bug (same pattern seems to work elsewhere)
    #lUsersOfInterest = _getpins("SELECT * FROM testphotos1:privilege AS p JOIN testphotos1:user AS u ON (p.tagont:hasVisibility = u.foaf:mbox) WHERE (p.tagont:hasTagLabel IN (%s));" % lFirstTagsStr)
    print ("users that have one of %s: %s" % (lFirstTagStr, [iU.get("http://code.google.com/p/tagont/hasVisibility") for iU in lUsersOfInterest]))
    def _countUserPhotos(_pUser):
        _lTagsP = _getpins("SELECT * FROM testphotos1:privilege WHERE tagont:hasVisibility='%s';" % _pUser)
        _lExpected_usPriv = lInMemoryChk.getTags_usPriv(_pUser)
        _lTags = set()
        for _iP in _lTagsP:
            _lTags.add(_iP["http://code.google.com/p/tagont/hasTagLabel"])
        print ("user %s has user privilege tags %s" % (_pUser, _lTags))
        if len(_lExpected_usPriv.difference(_lTags)) > 0:
            print ("WARNING: expected user-privilege tags %s" % _lExpected_usPriv.difference(_lTags))
            assert False
        _lTagsP = _getpins("SELECT * FROM testphotos1:privilege AS p JOIN testphotos1:user('%s') AS u ON (p.tagont:hasVisibility = u.foaf:\"member/adomain:Group\");" % _pUser)
        #_lTagsP = _getpins("SELECT * FROM testphotos1:privilege AS p JOIN testphotos1:user AS u ON (p.tagont:hasVisibility = u.foaf:\"member/adomain:Group\") WHERE u.foaf:mbox='%s';" % _pUser)
        #_lTagsP = _getpins("SELECT * FROM testphotos1:privilege WHERE tagont:hasVisibility='%s' UNION SELECT * FROM testphotos1:privilege AS p JOIN testphotos1:user AS u ON (p.tagont:hasVisibility = u.foaf:mbox) WHERE u.foaf:mbox='%s';" % (_pUser, _pUser)) # this segfaults... should check why
        for _iP in _lTagsP:
            _lTags.add(_iP["http://code.google.com/p/tagont/hasTagLabel"])
        print ("user %s has tags %s" % (_pUser, _lTags))
        _lExpectedTags = lInMemoryChk.getUserTags(_pUser)
        if len(_lExpectedTags.difference(_lTags)) > 0:
            print ("WARNING: expected tags %s" % _lExpectedTags.difference(_lTags))
            assert False
        _lPhotos = _getpins("SELECT * FROM testphotos1:photo AS p JOIN testphotos1:tagging AS t ON (p.semanticdesktop:\"nfo#hasHash\" = t.semanticdesktop:\"nfo#hasHash\") WHERE t.tagont:hasTagLabel IN (%s);" % ','.join([("'%s'" % _iT) for _iT in _lTags]))
        _lUniquePhotos = set()
        for _iP in _lPhotos:
            _lUniquePhotos.add(_iP["http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#hasHash"])
        if len(_lPhotos) != len(_lUniquePhotos):
            print ("non-unique: %s unique: %s" % (len(_lPhotos), len(_lUniquePhotos)))
        return len(_lUniquePhotos)
    for iU in lUsersOfInterest:
        lCntPhotos = _countUserPhotos(iU["http://code.google.com/p/tagont/hasVisibility"])
        _chkCount("photos that can be viewed by %s" % iU["http://code.google.com/p/tagont/hasVisibility"], _pExpected=lInMemoryChk.countPhotos(iU["http://code.google.com/p/tagont/hasVisibility"]), _pActual=lCntPhotos)
    if False:
        # Output the final state.
        print (_getpins("SELECT *;"))
    # Close everything.
    lAffinity.close()

class TestPhotos1b(AffinityTest):
    "A simple application talking to the store through pathSQL only, and using a relational model with joins."
    def execute(self):
        _entryPoint()
#AffinityTest.declare(TestPhotos1b)

if __name__ == '__main__':
    lT = TestPhotos1b()
    lT.execute()
