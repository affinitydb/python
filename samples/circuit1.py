#!/usr/bin/env python2.6
"""This module implements a simple 'circuit' design/simulation framework. It aims at demonstrating
ease of use thanks to mvstore references - both as a simple object mapping convenience,
and as a means of performing interesting join queries. It also shows how easily one can model
specific component characteristics, without cluttering the object model or schema. The sample also
tries to demonstrate that all this implies no compromise on performance."""
from mvstore import *
import logging
import modeling
import random

def safeLen(x):
    if x:
        return len(x)
    return 0
def safeList(pList):
    # TODO: Review why I sometimes lose 1-element collections...
    if not pList:
        return ()
    if not isinstance(pList, (tuple, list, MutableSequence)):
        return (pList, )
    return pList

class Component(object):
    """The base node used to construct circuits."""
    TYPES = ['AC-supply', 'AND', 'DC-supply', 'NAND', 'NOR', 'NOT', 'OR', 'XOR', 'ammeter', 'battery', 'bell', 'buzzer', 'capacitor', 'cell', 'diode', 'fuse', 'ground', 'heater', 'lamp', 'motor', 'ohmmeter', 'on-off switch', 'oscilloscope', 'push switch', 'relay', 'resistor', 'reversing switch', 'solenoid', 'transformer', 'transistor', 'variable capacitor', 'voltmeter', 'wire']
    def __init__(self, pPin):
        self.mPin = pPin
    def __repr__(self):
        return repr(self.mPin)
    pid = property(fget=lambda c: repr(c.mPin.mPID), doc="Component uid.")
    ctype = property(fget=lambda c: c.mPin["http://localhost/mv/property/1.0/circuit/component/type"], doc="Component type.")
    cx = property(fget=lambda c: c.mPin["http://localhost/mv/property/1.0/circuit/component/x"], doc="Component X position in the schema.")
    cy = property(fget=lambda c: c.mPin["http://localhost/mv/property/1.0/circuit/component/y"], doc="Component Y position in the schema.")
    inputs = property(fget=lambda c: c.mPin.get("http://localhost/mv/property/1.0/circuit/component/inputs"), doc="Component's input connections.")
    outputs = property(fget=lambda c: c.mPin.get("http://localhost/mv/property/1.0/circuit/component/outputs"), doc="Component's output connections.")
    @classmethod
    def create(cls, pType, pX, pY):
        return Component(PIN({"http://localhost/mv/property/1.0/circuit/component/type":pType, "http://localhost/mv/property/1.0/circuit/component/x":pX, "http://localhost/mv/property/1.0/circuit/component/y":pY}).savePIN())

class Circuit(object):
    """A container for a given circuit. Uses a collection."""
    DISPLAY_WIDTH = 100
    DISPLAY_HEIGHT = 50
    DISPLAY_MARGIN = 20
    def __init__(self, pName):
        # Either retrieve the circuit pin by query, if it already exists...
        self.mMvStore = MVSTORE()
        lPins = PIN.loadPINs(self.mMvStore.qProto("SELECT * FROM \"http://localhost/mv/class/1.0/Circuit\"('%s');" % pName))
        self.mComponentsCache = {} # Review: for multi-user, may want to refine/remove this.
        if lPins != None and len(lPins) > 0:
            self.mPin = lPins[0]
            for iC in self.mPin["http://localhost/mv/property/1.0/circuit/components"]:
                lC = Component(PIN.createFromPID(iC))
                self.mComponentsCache[repr(lC.mPin.mPID)] = lC
        # Or create a new circuit.
        else:
            self.mPin = PIN({"http://localhost/mv/property/1.0/circuit/name":pName}).savePIN() # Note: It doesn't matter that we don't yet have components.
    def addComponents(self, pWidth, pHeight, pDensity):
        "Randomly create a patch of inter-connected components."
        self.mComponentsCache.clear()
        self.mMvStore.startTx()
        # Create the components (randomly, on a grid).
        print ("Inserting components...")
        if False:
            for iY in xrange(pHeight):
                for iX in xrange(pWidth):
                    lC = Component.create(random.choice(Component.TYPES), \
                        Circuit.DISPLAY_MARGIN + iX * (Circuit.DISPLAY_WIDTH + Circuit.DISPLAY_MARGIN), \
                        Circuit.DISPLAY_MARGIN + iY * (Circuit.DISPLAY_HEIGHT + Circuit.DISPLAY_MARGIN))
                    self.mPin.addElement("http://localhost/mv/property/1.0/circuit/components", PIN.Ref.fromPID(lC.mPin.mPID))
                    self.mComponentsCache[repr(lC.mPin.mPID)] = lC
        else:
            lPins = []
            for iY in xrange(pHeight):
                for iX in xrange(pWidth):
                    lPins.append(PIN({
                        "http://localhost/mv/property/1.0/circuit/component/type":random.choice(Component.TYPES), 
                        "http://localhost/mv/property/1.0/circuit/component/x":Circuit.DISPLAY_MARGIN + iX * (Circuit.DISPLAY_WIDTH + Circuit.DISPLAY_MARGIN), 
                        "http://localhost/mv/property/1.0/circuit/component/y":Circuit.DISPLAY_MARGIN + iY * (Circuit.DISPLAY_HEIGHT + Circuit.DISPLAY_MARGIN)}))
            PIN.savePINs(lPins)
            if True:
                for iP in lPins:
                    self.mPin.addElement("http://localhost/mv/property/1.0/circuit/components", PIN.Ref.fromPID(iP.mPID))
                    self.mComponentsCache[repr(iP.mPID)] = Component(iP)
                self.mMvStore.mTxCtx.flush() # REVIEW: automate like in nodejs...
        # Create connections among them, according to pDensity.
        # Prefer nearby connections first.
        print ("Inserting connections...")
        lNumConnections = int(float(pHeight) * float(pWidth) * pDensity)
        for iC in xrange(lNumConnections):
            lComp1 = random.choice(self.mComponentsCache.values())
            lComp2 = lComp1
            while lComp2.mPin.mPID == lComp1.mPin.mPID:
                lNumConnC1 = safeLen(lComp1.inputs) + safeLen(lComp1.outputs)
                if lNumConnC1 > 3:
                    lComp2 = random.choice(self.mComponentsCache.values())
                else:
                    lExcluded = [lComp1.mPin.mPID]
                    if lComp1.inputs:
                        lExcluded.extend(lComp1.inputs)
                    if lComp1.outputs:
                        lExcluded.extend(lComp1.outputs)
                    lExcludedStr = ','.join(["'%s'" % iE for iE in lExcluded])
                    lMinX = lComp1.cx - Circuit.DISPLAY_WIDTH - Circuit.DISPLAY_MARGIN
                    lMaxX = lComp1.cx + 2 * Circuit.DISPLAY_WIDTH + Circuit.DISPLAY_MARGIN
                    lMinY = lComp1.cy - Circuit.DISPLAY_HEIGHT - Circuit.DISPLAY_MARGIN
                    lMaxY = lComp1.cy + 2 * Circuit.DISPLAY_HEIGHT + Circuit.DISPLAY_MARGIN
                    lCondition = " WHERE ((cp.mv:pinID NOT IN (%s)) AND (cp.\"http://localhost/mv/property/1.0/circuit/component/x\" IN [%s, %s]) AND (cp.\"http://localhost/mv/property/1.0/circuit/component/y\" IN [%s, %s]))" % (lExcludedStr, lMinX, lMaxX, lMinY, lMaxY)
                    # TODO: enable when bug 108 is fixed.
                    #lCondition = " WHERE (cp.mv:pinID NOT IN (%s))" % lExcludedStr
                    lCandidates = PIN.loadPINs(self.mMvStore.qProto( \
                        "SELECT * FROM \"http://localhost/mv/class/1.0/Circuit/Component#bypos\" AS cp JOIN \"http://localhost/mv/class/1.0/Circuit\"('%s') AS c ON (cp.mv:pinID = c.\"http://localhost/mv/property/1.0/circuit/components\")%s;" % \
                        (self.mPin["http://localhost/mv/property/1.0/circuit/name"], lCondition)))
                        # TODO: enable when bug 108 is fixed.
                        #"SELECT * FROM \"http://localhost/mv/class/1.0/Circuit/Component#bypos\"([%s, %s], [%s, %s]) AS cp JOIN \"http://localhost/mv/class/1.0/Circuit\"('%s') AS c ON (cp.mv:pinID = c.\"http://localhost/mv/property/1.0/circuit/components\")%s;" % \
                        #(lMinX, lMaxX, lMinY, lMaxY, self.mPin["http://localhost/mv/property/1.0/circuit/name"], lCondition))))
                    if 0 == len(lCandidates):
                        logging.debug("couldn't find a candidate for %s with exclusions %s" % (lComp1.mPin.mPID, lExcludedStr))
                        lComp2 = None
                        break
                    logging.debug("candidates for (%s,%s): %s" % (lComp1.cx, lComp1.cy, ' '.join(["(%s,%s)" % (iO["http://localhost/mv/property/1.0/circuit/component/x"], iO["http://localhost/mv/property/1.0/circuit/component/y"]) for iO in lCandidates])))
                    lComp2p = random.choice(lCandidates)
                    lComp2 = None
                    if lComp2p:
                        lComp2 = self.mComponentsCache[repr(lComp2p.mPID)]
            if lComp2 and not lComp2.mPin.mPID == lComp1.mPin.mPID:
                lComp1.mPin.addElement("http://localhost/mv/property/1.0/circuit/component/outputs", PIN.Ref.fromPID(lComp2.mPin.mPID))
                lComp2.mPin.addElement("http://localhost/mv/property/1.0/circuit/component/inputs", PIN.Ref.fromPID(lComp1.mPin.mPID))
                logging.info("connected %s to %s" % (lComp1.pid, lComp2.pid))
        print ("Committing...")
        self.mMvStore.commitTx()
        print ("Done.")
    def connectComponent(self, pC, pTo):
        "Connect the component pC to every component in pTo."
        lOutputs = safeList(pC.outputs)
        for iTo in pTo:
            if iTo.pid in lOutputs:
                continue
            pC.mPin.addElement("http://localhost/mv/property/1.0/circuit/component/outputs", PIN.Ref.fromPID(iTo.mPin.mPID))
            lC = self.mComponentsCache[iTo.pid]
            lC.mPin.addElement("http://localhost/mv/property/1.0/circuit/component/inputs", PIN.Ref.fromPID(pC.mPin.mPID))
            logging.info("connected %s to %s" % (pC.pid, iTo.pid))
    def disconnectComponent(self, pC):
        "Disconnect the component pC from every component it is connected to."
        for iC in safeList(pC.inputs):
            lOutputs = self.mComponentsCache[repr(iC)].outputs
            if lOutputs and isinstance(lOutputs, PIN.Collection) and len(lOutputs) > 1:
                lOutputs.remove(pC.pid)
            elif lOutputs:
                del self.mComponentsCache[repr(iC)].mPin["http://localhost/mv/property/1.0/circuit/component/outputs"]
        for iC in safeList(pC.outputs):
            lInputs = self.mComponentsCache[repr(iC)].inputs
            if lInputs and isinstance(lInputs, PIN.Collection) and len(lInputs) > 1:
                lInputs.remove(pC.pid)
            elif lInputs:
                del self.mComponentsCache[repr(iC)].mPin["http://localhost/mv/property/1.0/circuit/component/inputs"]
        if pC.mPin.has_key("http://localhost/mv/property/1.0/circuit/component/outputs"):
            del pC.mPin["http://localhost/mv/property/1.0/circuit/component/outputs"]
        if pC.mPin.has_key("http://localhost/mv/property/1.0/circuit/component/inputs"):
            del pC.mPin["http://localhost/mv/property/1.0/circuit/component/inputs"]
    def deleteComponent(self, pC):
        "Disconnect and delete the component pC."
        self.disconnectComponent(pC)
        del self.mComponentsCache[pC.pid]
        self.mPin["http://localhost/mv/property/1.0/circuit/components"].remove(pC.pid)
        pC.mPin.deletePIN()
    def setName(self, pName):
        "Change the name of the circuit."
        self.mPin["http://localhost/mv/property/1.0/circuit/name"] = pName
    name = property(fget=lambda c: c.mPin["http://localhost/mv/property/1.0/circuit/name"], fset=setName, doc="Circuit's name.")
    components = property(fget=lambda c: c.mComponentsCache, doc="Components of the circuit.")
    @classmethod
    def declareMvClasses(cls):
        "Declare the mvstore classes used by the implementation."
        try:
            MVSTORE().qProto("CREATE CLASS \"http://localhost/mv/class/1.0/Circuit\" AS SELECT * WHERE \"http://localhost/mv/property/1.0/circuit/name\" IN :0 AND EXISTS(\"http://localhost/mv/property/1.0/circuit/components\");")
            MVSTORE().qProto("CREATE CLASS \"http://localhost/mv/class/1.0/Circuit/Component#bypos\" AS SELECT * WHERE \"http://localhost/mv/property/1.0/circuit/component/x\" IN :0(int) AND \"http://localhost/mv/property/1.0/circuit/component/y\" IN :1(int);")
        except:
            pass
    @classmethod
    def describeModel(cls):
        "Describe the model, using the modeling extension."
        modeling.initialize()
        modeling.describeClass("http://localhost/mv/class/1.0/Circuit", {modeling.MVURI_PROP_DOCSTRING:"[sample] Category for circuits, indexed by name."})
        modeling.describeClass("http://localhost/mv/class/1.0/Circuit/Component#bypos", {modeling.MVURI_PROP_DOCSTRING:"[sample] Category for circuits' components, indexed by 2D-position."})
        modeling.describeRelation("http://localhost/mv/property/1.0/circuit/components", {modeling.MVURI_PROP_DOCSTRING:"[sample] Collection of components.", modeling.MVURI_PROP_CATEGORY:"http://localhost/mv/class/1.0/Circuit/Component#bypos"})
        modeling.describeRelation("http://localhost/mv/property/1.0/circuit/components", {modeling.MVURI_PROP_DOCSTRING:"[sample] A circuit has a collection of components.", modeling.MVURI_PROP_CARDINALITY:modeling.MV_CARDINALITY_ONE_TO_MANY}, pClassURI="http://localhost/mv/class/1.0/Circuit")
        modeling.describeRelation("http://localhost/mv/property/1.0/circuit/component/inputs", {modeling.MVURI_PROP_DOCSTRING:"[sample] Collection of components connected as inputs.", modeling.MVURI_PROP_CATEGORY:"http://localhost/mv/class/1.0/Circuit/Component#bypos"})
        modeling.describeRelation("http://localhost/mv/property/1.0/circuit/component/inputs", {modeling.MVURI_PROP_DOCSTRING:"[sample] Collection of components connected as inputs.", modeling.MVURI_PROP_CARDINALITY:modeling.MV_CARDINALITY_ONE_TO_MANY}, pClassURI="http://localhost/mv/class/1.0/Circuit/Component#bypos")
        modeling.describeRelation("http://localhost/mv/property/1.0/circuit/component/outputs", {modeling.MVURI_PROP_DOCSTRING:"[sample] Collection of components connected as outputs.", modeling.MVURI_PROP_CATEGORY:"http://localhost/mv/class/1.0/Circuit/Component#bypos"})
        modeling.describeRelation("http://localhost/mv/property/1.0/circuit/component/outputs", {modeling.MVURI_PROP_DOCSTRING:"[sample] Collection of components connected as outputs.", modeling.MVURI_PROP_CARDINALITY:modeling.MV_CARDINALITY_ONE_TO_MANY}, pClassURI="http://localhost/mv/class/1.0/Circuit/Component#bypos")
        modeling.describeAttribute("http://localhost/mv/property/1.0/circuit/name", {modeling.MVURI_PROP_DOCSTRING:"[sample] Circuit's name."}, pClassURI="http://localhost/mv/class/1.0/Circuit")
        modeling.describeAttribute("http://localhost/mv/property/1.0/circuit/component/x", {modeling.MVURI_PROP_DOCSTRING:"[sample] Component's x position."}, pClassURI="http://localhost/mv/class/1.0/Circuit/Component#bypos")
        modeling.describeAttribute("http://localhost/mv/property/1.0/circuit/component/y", {modeling.MVURI_PROP_DOCSTRING:"[sample] Component's y position."}, pClassURI="http://localhost/mv/class/1.0/Circuit/Component#bypos")
        modeling.describeAttribute("http://localhost/mv/property/1.0/circuit/component/type", {modeling.MVURI_PROP_DOCSTRING:"[sample] Component's type."}, pClassURI="http://localhost/mv/class/1.0/Circuit/Component#bypos")
