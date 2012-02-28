#!/usr/bin/env python2.6
# Copyright (c) 2004-2012 VMware, Inc. All Rights Reserved.
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
"""This module provides common services for the testphotos*.py series of tests."""
from copy import copy

class InMemoryChk(object):
    """This is the main validation for this test (which is arguably more just a small sample
    than it really is a test)."""
    def __init__(self):
        self.mPhotos = {} # Dictionary of photoid, set(tags)
        self.mUsers = {} # Dictionary of userid, {set(tags), group}
        self.mGroups = {} # Dictionary of groupid, set(tags)
    def tagPhoto(self, pPhoto, pTag):
        if not self.mPhotos.has_key(pPhoto):
            self.mPhotos[pPhoto] = set()
        self.mPhotos[pPhoto].add(pTag)
    def setUserGroup(self, pUser, pGroup):
        if not self.mUsers.has_key(pUser):
            self.mUsers[pUser] = {}
        if not self.mGroups.has_key(pGroup):
            self.mGroups[pGroup] = set()
        self.mUsers[pUser]["group"] = pGroup
    def addUserPrivilege(self, pUser, pTag):
        if not self.mUsers.has_key(pUser):
            self.mUsers[pUser] = {}
        if not self.mUsers[pUser].has_key("tags"):
            self.mUsers[pUser]["tags"] = set()
        self.mUsers[pUser]["tags"].add(pTag)
    def addGroupPrivilege(self, pGroup, pTag):
        if not self.mGroups.has_key(pGroup):
            self.mGroups[pGroup] = set()
        self.mGroups[pGroup].add(pTag)
    def getTags_usPriv(self, pUser):
        if not self.mUsers.has_key(pUser):
            return 0
        return copy(self.mUsers[pUser]["tags"])
    def getTags_grPriv(self, pUser):
        if not self.mUsers.has_key(pUser):
            return 0
        return copy(self.mGroups[self.mUsers[pUser]["group"]])
    def getUserTags(self, pUser):
        return self.getTags_usPriv(pUser).union(self.getTags_grPriv(pUser))
    def countPhotos(self, pUser):
        if not self.mUsers.has_key(pUser):
            return 0
        lTags = self.getUserTags(pUser)
        lPhotos = set()
        for iP in self.mPhotos.items():
            for iT in iP[1]:
                if iT in lTags:
                    lPhotos.add(iP[0])
        return len(lPhotos)
