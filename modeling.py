#!/usr/bin/env python2.6
"""This module defines services that help overlay useful information (semantic adornments, documentation)
on top of any schema. It also leverages some of mvstore's native introspection services. The intent is to
demonstrate and encourage a physical modeling that is semantically rich and facilitates interoperability,
even between programmers or modelers that don't know anything about each other. The erdiagram.py sample
will interpret this information and present live ER diagrams.
Note: In the future, some of this functionality will migrate toward the kernel."""
from __future__ import with_statement
from mvstore import *
import logging

# TODO: transaction around check + creation pattern
# TODO: document that can be called before...
# TODO: support qnames

MVURI_VERSION_SUFFIX = "1.0"
MVURI_CLASS_OF_ATTRIBUTE_DESCR = "http://localhost/mv/class/%s/AttributeDescription" % MVURI_VERSION_SUFFIX
MVURI_CLASS_OF_RELATION_DESCR = "http://localhost/mv/class/%s/RelationDescription" % MVURI_VERSION_SUFFIX
MVURI_CLASS_OF_CLASS_DESCR = "http://localhost/mv/class/%s/ClassDescription" % MVURI_VERSION_SUFFIX
MVURI_PROP_URI_OF_ATTRIBUTE = "http://localhost/mv/property/%s/describesAttribute" % MVURI_VERSION_SUFFIX
MVURI_PROP_URI_OF_RELATION = "http://localhost/mv/property/%s/describesRelation" % MVURI_VERSION_SUFFIX
MVURI_PROP_URI_OF_CLASS = "http://localhost/mv/property/%s/describesClass" % MVURI_VERSION_SUFFIX
MVURI_PROP_DOCSTRING = "http://localhost/mv/property/%s/hasDocstring" % MVURI_VERSION_SUFFIX
MVURI_PROP_CARDINALITY = "http://localhost/mv/property/%s/hasCardinality" % MVURI_VERSION_SUFFIX
MVURI_PROP_CATEGORY = "http://localhost/mv/property/%s/withClass" % MVURI_VERSION_SUFFIX # The description of a relation can contain more than one of these.
MV_CARDINALITY_ONE_TO_ONE = "1..1"
MV_CARDINALITY_ONE_TO_MANY = "1..n"
MV_CARDINALITY_MANY_TO_MANY = "n..n"

def initialize():
    "Initialize the modeling/introspection library."
    try:
        # Review: Use a qname and namespace, as soon as it becomes possible.
        # Note: the attribute/relation must exist, but the class may be NULL (in which case this PIN describes the global attribute/relation).
        # Review: remove those 'NOT EXISTS' when DISTINCT is available...
        MVSTORE().qProto("CREATE CLASS \"%s\" AS SELECT * WHERE \"%s\" IN :0;" % \
          (MVURI_CLASS_OF_ATTRIBUTE_DESCR, MVURI_PROP_URI_OF_ATTRIBUTE))
        MVSTORE().qProto("CREATE CLASS \"%s\" AS SELECT * WHERE \"%s\" IN :0;" % \
          (MVURI_CLASS_OF_RELATION_DESCR, MVURI_PROP_URI_OF_RELATION))
        MVSTORE().qProto("CREATE CLASS \"%s\" AS SELECT * WHERE \"%s\" IN :0 AND NOT EXISTS(\"%s\") AND NOT EXISTS(\"%s\");" % \
          (MVURI_CLASS_OF_CLASS_DESCR, MVURI_PROP_URI_OF_CLASS, MVURI_PROP_URI_OF_ATTRIBUTE, MVURI_PROP_URI_OF_RELATION))
    except:
        pass

def _describeProperty(pURI, pDict, pIsRelation, pClassURI=None):
    "[internal] Describe the property designated by pURI, using the properties and values of pDict; pClassURI determines if the description is in the scope of a class."
    lClassType = (MVURI_CLASS_OF_ATTRIBUTE_DESCR, MVURI_CLASS_OF_RELATION_DESCR)[pIsRelation]
    if pClassURI:
        lCheck = MVSTORE().q("SELECT * FROM \"%s\"('%s') WHERE (\"%s\" = '%s');" % \
            (lClassType, pURI, MVURI_PROP_URI_OF_CLASS, pClassURI), pFlags=1)
    else:
        lCheck = MVSTORE().q("SELECT * FROM \"%s\"('%s') WHERE NOT EXISTS(\"%s\");" % \
            (lClassType, pURI, MVURI_PROP_URI_OF_CLASS), pFlags=1)
    if lCheck > 0:
        # TODO: validations.
        return
    else:
        lKey = (MVURI_PROP_URI_OF_ATTRIBUTE, MVURI_PROP_URI_OF_RELATION)[pIsRelation]
        lPin = PIN({lKey:pURI})
        if pClassURI:
            lPin[MVURI_PROP_URI_OF_CLASS] = pClassURI
        for iP in pDict.items():
            lPin[iP[0]] = iP[1]
        lPin.savePIN()

def describeAttribute(pURI, pDict, pClassURI=None):
    "Describe the attribute designated by pURI, using the properties and values of pDict; pClassURI determines if the description is in the scope of a class."
    _describeProperty(pURI, pDict, False, pClassURI)

def describeRelation(pURI, pDict, pClassURI=None):
    "Describe the relation designated by pURI, using the properties and values of pDict; pClassURI determines if the description is in the scope of a class."
    _describeProperty(pURI, pDict, True, pClassURI)

def describeClass(pURI, pDict):
    # Note: For the moment, I use a separate PIN (than the actual class PIN), to store the description; this also provides independence in terms of when these functions are called.
    # Note: We don't expect an enumeration of properties or of related classes in pDict, since this is already automated in the kernel.
    lCheck = MVSTORE().q("SELECT * FROM \"%s\"('%s');" % (MVURI_CLASS_OF_CLASS_DESCR, pURI), pFlags=1)
    if lCheck > 0:
        # TODO: validations.
        return
    else:
        lPin = PIN({MVURI_PROP_URI_OF_CLASS:pURI})
        for iP in pDict.items():
            lPin[iP[0]] = iP[1]
        lPin.savePIN()

def describeSchema(pSchema, pQNames, **kwargs):
    # provide an opportunity to describe a context that brings together a number of classes and properties.
    pass

class ERSchema(object):
    """ERSchema is an in-memory representation of entities (classes) and relationships (references/fk)
    built from information retrieved in the store, including native classes themselves, as well as
    meta-data added via the modeling extension."""
    class Relation(object):
        """'Relation' refers directly to what 'describeRelation' produces."""
        def __init__(self, pURI, pFk, pFkTarget, pPin=None):
            # Note: I'm using the 'foreign key' terminology/analogy, to avoid any possible confusion.
            self.mURI = pURI # Full URI of the mvstore property that holds this relation (or, eventually, could be the URI of an abstraction of a join by foreign key).
            self.mFk = pFk # The 'Entity' that is 'contained', or holds the foreign key pointing back at 'pFkTarget' (MVURI_PROP_CATEGORY).
            self.mFkTarget = pFkTarget # The 'Entity' that 'contains', or is pointed to by 'pFk' (MVURI_PROP_URI_OF_CLASS).
            self.mPin = pPin # Actual pin in the store representing the relation (MVURI_CLASS_OF_RELATION_DESCR).
    class Attribute(object):
        """'Attribute' refers to the union of properties enumerated by an entity's 'mv:properties',
        and other properties declared via 'describeAttribute'."""
        def __init__(self, pURI, pPin=None):
            self.mURI = pURI # Full URI of the mvstore property that holds this attribute.
            self.mPin = pPin # Actual pin in the store representing the attribute (MVURI_CLASS_OF_ATTRIBUTE_DESCR).
    class Entity(object):
        """'Entity' is synonymous with class/family, in this context. Note that unlike in formal ER diagrams,
        our entities represent things that are extensible (i.e. not constrained by [or reduced to] the
        definitions of the entities themselves)."""
        def __init__(self, pClassURI, pPin=None):
            self.mClassURI = pClassURI # Full URI of the class/family represented by this 'Entity' object.
            self.mPin = pPin # Actual pin in the store representing the native class (not necessarily MVURI_CLASS_OF_CLASS_DESCR).
            self.mRelations = [] # Inbound and outbound relations.
            self.mAttributes = {} # All attributes.
        def __repr__(self):
            lP = []
            for iR in self.mRelations:
                if self == iR.mFkTarget:
                    lP.append('*%s' % iR.mURI)
            lP.extend(['%s' % iA for iA in self.mAttributes.keys()])
            return "%s (%s)" % (self.mClassURI, ','.join(lP))
    def __init__(self, pSchema=None, pQNames=None):
        # TODO: Respect pSchema and pQNames...
        self.mMvStore = MVSTORE()
        self.mQNames = pQNames
        self.mPaths2QNames = {}
        self.mClasses = PIN.loadPINs(self.mMvStore.qProto("SELECT * FROM mv:ClassOfClasses;"))
        self.mEntities = {}
        # Create the 'Entity' objects.
        for iC in self.mClasses:
            if 0 == iC[SP_PROPERTY_NAMES[mvstore_pb2.SP_CLASSID]]:
                continue
            lClassURI = self._withQName(iC[SP_PROPERTY_NAMES[mvstore_pb2.SP_CLASSID]])
            self.mEntities[lClassURI] = ERSchema.Entity(lClassURI, iC)
        # Create the 'Attribute' and 'Relation' objects, for each 'Entity'.
        for iE in self.mEntities.values():
            # Find out what properties are implicitly 'known' by the class/family.
            lProperties = set()
            if SP_PROPERTY_NAMES[mvstore_pb2.SP_PROPERTIES] in iE.mPin:
                for iProp in iE.mPin[SP_PROPERTY_NAMES[mvstore_pb2.SP_PROPERTIES]]:
                    lProperties.add(self._withQName(iProp))
            # Deal with the relations.
            if True:
                #lPropertiesStr = ','.join(["'%s'" % iEP for iEP in lProperties])
                #lRelations = PIN.loadPINs(self.mMvStore.qProto("SELECT * FROM %s AS r1 JOIN mv:ClassOfClasses AS e1 ON (r1.%s = e1.mv:properties) WHERE (e1.mv:classID='%s');" % \
                #lRelations = PIN.loadPINs(self.mMvStore.qProto("SELECT * FROM %s WHERE %s IN (%s);" % \
                #    (MVURI_CLASS_OF_RELATION_DESCR, MVURI_PROP_URI_OF_RELATION, lPropertiesStr)))
                lRelations = PIN.loadPINs(self.mMvStore.qProto("SELECT * FROM \"%s\" WHERE \"%s\"='%s';" % \
                    (MVURI_CLASS_OF_RELATION_DESCR, MVURI_PROP_URI_OF_CLASS, self._withoutQName(iE.mClassURI))))
                logging.info("relations for entity %s: %s" % (iE.mClassURI, ','.join(['%s' % self._withQName(iR[MVURI_PROP_URI_OF_RELATION]) for iR in lRelations])))
                for iR in lRelations:
                    lRelationURI = iR[MVURI_PROP_URI_OF_RELATION]
                    lBase = PIN.loadPINs(self.mMvStore.qProto("SELECT * FROM \"%s\"('%s') WHERE NOT EXISTS(\"%s\");" % \
                        (MVURI_CLASS_OF_RELATION_DESCR, lRelationURI, MVURI_PROP_URI_OF_CLASS)))
                    lTo = []
                    for iB in lBase:
                        if iB.has_key(MVURI_PROP_CATEGORY):
                            ERSchema._appendOrExtend(lTo, self._withQName(iB[MVURI_PROP_CATEGORY]))
                    if iR.has_key(MVURI_PROP_CATEGORY):
                        ERSchema._appendOrExtend(lTo, self._withQName(iR[MVURI_PROP_CATEGORY]))
                    for iTo in lTo:
                        logging.info("adding relation %s to entity %s" % (lRelationURI, iE.mClassURI))
                        lFk = self.mEntities[iTo]
                        lRelation = ERSchema.Relation(pURI=self._withQName(lRelationURI), pFk=lFk, pFkTarget=iE, pPin=iR)
                        iE.mRelations.append(lRelation)
                        if iE != lFk:
                            lFk.mRelations.append(lRelation)
                    lProperties.discard(lRelationURI)
            # Deal with the attributes.
            if True:
                lAttributes = PIN.loadPINs(self.mMvStore.qProto("SELECT * FROM \"%s\" WHERE \"%s\"='%s';" % \
                    (MVURI_CLASS_OF_ATTRIBUTE_DESCR, MVURI_PROP_URI_OF_CLASS, self._withoutQName(iE.mClassURI))))
                logging.info("declared attributes for entity %s: %s" % (iE.mClassURI, ','.join(['%s' % iA[MVURI_PROP_URI_OF_ATTRIBUTE] for iA in lAttributes])))
                for iA in lAttributes:
                    lAttributeURI = self._withQName(iA[MVURI_PROP_URI_OF_ATTRIBUTE])
                    lAttribute = ERSchema.Attribute(pURI=lAttributeURI, pPin=iA)
                    iE.mAttributes[lAttributeURI] = lAttribute
                    lProperties.discard(lAttributeURI)
                logging.info("other implicit attributes for entity %s: %s" % (iE.mClassURI, ','.join(['%s' % iA for iA in lProperties])))
                for iA in lProperties:
                    lAttribute = ERSchema.Attribute(pURI=iA)
                    iE.mAttributes[iA] = lAttribute
    def __repr__(self):
        return "ERSchema: {\n  %s\n}\nQNames: {\n  %s\n}" % ('\n  '.join([repr(iE) for iE in self.mEntities.values()]), '\n  '.join(["%s=%s" % (iQN[0], iQN[1]) for iQN in self.mQNames.items()]))
    def _withQName(self, pName):
        if None == self.mQNames:
            return pName
        lLastSlash = pName.rfind("/")
        if lLastSlash < 0:
            return pName
        if not self.mPaths2QNames.has_key(pName[:lLastSlash]):
            lNewQName = "qn%s" % len(self.mPaths2QNames.keys())
            self.mPaths2QNames[pName[:lLastSlash]] = lNewQName
            self.mQNames[lNewQName] = pName[:lLastSlash]
        return "%s:%s" % (self.mPaths2QNames[pName[:lLastSlash]], pName[lLastSlash + 1:])
    def _withoutQName(self, pName):
        if None == self.mQNames:
            return pName
        lColon = pName.find(":")
        if lColon < 0:
            return pName
        if self.mQNames.has_key(pName[:lColon]):
            return "%s/%s" % (self.mQNames[pName[:lColon]], pName[lColon + 1:])
        return pName
    size = property(fget=lambda s: len(s.mEntities.keys()), doc="Size of the schema, i.e. number of entities in the schema.")
    @staticmethod
    def _appendOrExtend(pContainer, pWhat):
        if isinstance(pWhat, (list, tuple, MutableSequence)):
            pContainer.extend(pWhat)
        else:
            pContainer.append(pWhat)

def extractERSchema(pSchema=None, pQNames=None):
    return ERSchema(pSchema, pQNames)
