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
"""This module implements a UI for the 'circuit1' sample. It aims at providing an immediate
user experience, allowing to better assess the demonstrated capabilities (in terms of
responsiveness, feature-richness etc.)."""
try:
    from tkinter import * # For UI.
except:
    from Tkinter import * # For UI.
from affinity import * # For Affinity db access.
from uihelpers import * # For simple tkinter helpers.
from circuit1 import * # For our circuit model.

# TODO: investigate freeze on shutdown (scenario: new 25x25 circuit with 0 connection; exit -> freeze)
  # . does not repro without the components_bypos class
  # . does not repro without the components collection
  # . but does not repro either with a simple c++ test mimicking everything (but not going through proto...)
# TODO: investigate performance of creation... seems much slower than equivalent pure c++ test... tx works? python overhead? need batching?
# TODO: move around, tag etc.
# TODO: maybe offer 2 modes: immediate save (e.g. button up etc.), and onidle save (log-based)
# TODO: demo multi-user? without notifs?

class CircuitUI(object):
    "Manage the main canvas, to represent a circuit."
    def __init__(self, pCanvas, pCircuit=None):
        self.mCanvas = pCanvas
        self.mCircuit = pCircuit
        self.mSelected = None # A tuple (component, UI id).
        self.mSoftSelected = [] # A list of tuples (component, UI id).
    def setCircuit(self, pCircuit):
        self.clear()
        self.mCircuit = pCircuit
        lMaxX = lMaxY = 0
        for iC in pCircuit.components.values():
            self.mCanvas.create_rectangle(iC.cx, iC.cy, iC.cx + Circuit.DISPLAY_WIDTH, iC.cy + Circuit.DISPLAY_HEIGHT, tags=("component", "crect", iC.pid), fill="#cccccc")
            lMaxX = max(lMaxX, iC.cx + Circuit.DISPLAY_WIDTH)
            lMaxY = max(lMaxY, iC.cy + Circuit.DISPLAY_HEIGHT)
        for iC in pCircuit.components.values():
            self.mCanvas.create_text(iC.cx + 2, iC.cy + 2, text=iC.ctype, anchor=NW, tags=("component", "ctext"))
        for iC in pCircuit.components.values():
            if not iC.outputs:
                continue
            for iConn in safeList(iC.outputs):
                lDst = pCircuit.components[repr(iConn)]
                self._createConnection(iC, lDst)
        self.mCanvas.configure(scrollregion=(0, 0, lMaxX+Circuit.DISPLAY_MARGIN, lMaxY+Circuit.DISPLAY_MARGIN))
    def refresh(self):
        self.setCircuit(self.mCircuit)
    def unselect(self, pSoft=False):
        if pSoft and self.mSoftSelected != None:
            for iS in self.mSoftSelected:
                self.mCanvas.itemconfigure(iS[1], outline="#000000")
            self.mSoftSelected = []
        elif self.mSelected != None:
            self.mCanvas.itemconfigure(self.mSelected[1], outline="#000000")
            self.mSelected = None
    def selectComponent(self, pX, pY, pCanvasCoords=False, pSoft=False):
        lS = self.findComponent(pX, pY, pCanvasCoords)
        if None == lS:
            return
        if pSoft:
            self.mSoftSelected.append(lS)
            self.mCanvas.itemconfigure(lS[1], outline="#0000ff")
        else:
            self.mSelected = lS
            self.mCanvas.itemconfigure(lS[1], outline="#ff0000")
    def findComponent(self, pX, pY, pCanvasCoords=False):
        lX = (self.mCanvas.canvasx(pX), pX)[pCanvasCoords]
        lY = (self.mCanvas.canvasy(pY), pY)[pCanvasCoords]
        lId = self.mCanvas.find_closest(lX, lY, start="ctext")
        if lId == None:
            return None
        for iT in self.mCanvas.gettags(lId):
            if iT[0] == "@":
                return (self.mCircuit.components[iT], lId)
        return None
    def clear(self):
        self.mSelected = None
        self.mSoftSelected = []
        lAllC = self.mCanvas.find_all()
        for iC in lAllC:
            self.mCanvas.delete(iC)
    def _createConnection(self, pComponent1, pComponent2):
        # Align the actual positions on a grid, to simplify things (to simplify handling of components displaced by the user).
        def __2log(x=0, y=0):
            return (x - Circuit.DISPLAY_MARGIN) / (Circuit.DISPLAY_WIDTH + Circuit.DISPLAY_MARGIN), (y - Circuit.DISPLAY_MARGIN) / (Circuit.DISPLAY_HEIGHT + Circuit.DISPLAY_MARGIN)
        def __2phy(x=0, y=0):
            return Circuit.DISPLAY_MARGIN + x * (Circuit.DISPLAY_WIDTH + Circuit.DISPLAY_MARGIN), Circuit.DISPLAY_MARGIN + y * (Circuit.DISPLAY_HEIGHT + Circuit.DISPLAY_MARGIN)
        lX1, lY1 = __2log(pComponent1.cx, pComponent1.cy)
        lX2, lY2 = __2log(pComponent2.cx, pComponent2.cy)
        # Choose the color.
        lRed = min(abs(lX2 - lX1) * 150, 255)
        lGreen = min(abs(lY2 - lY1) * 150, 255)
        lColor = "#%02x%02x77" % (lRed, lGreen)
        # Create the segments.
        lPoints = []
        lPoints.append([pComponent1.cx + Circuit.DISPLAY_WIDTH, pComponent1.cy + (Circuit.DISPLAY_HEIGHT / 2)])
        lPoints.append([__2phy(x=lX1)[0] + Circuit.DISPLAY_WIDTH + 3, __2phy(y=lY1)[1] + (Circuit.DISPLAY_HEIGHT / 2)])
        if lX2 == lX1 + 1:
            lPoints.append([lPoints[-1][0], __2phy(y=lY2)[1] + (Circuit.DISPLAY_HEIGHT / 2)])
        elif lX2 > lX1:
            lPoints.append([lPoints[-1][0], __2phy(y=lY2)[1] + Circuit.DISPLAY_HEIGHT + 3])
            lPoints.append([__2phy(x=lX2)[0] - 3, lPoints[-1][1]])
            lPoints.append([lPoints[-1][0], __2phy(y=lY2)[1] + (Circuit.DISPLAY_HEIGHT / 2)])
        else:
            lPoints.append([lPoints[-1][0], __2phy(y=lY2)[1] - 3])
            lPoints.append([__2phy(x=lX2)[0] - 3, lPoints[-1][1]])
            lPoints.append([lPoints[-1][0], __2phy(y=lY2)[1] + (Circuit.DISPLAY_HEIGHT / 2)])
        lPoints.append([pComponent2.cx, pComponent2.cy + (Circuit.DISPLAY_HEIGHT / 2)])
        for iP in xrange(len(lPoints)):
            if iP == 0:
                continue
            self.mCanvas.create_line(lPoints[iP - 1][0], lPoints[iP - 1][1], lPoints[iP][0], lPoints[iP][1], fill="#dd0000", tags="connection")
        
if __name__ == '__main__':
    # Have a db connection.
    lAffinity = AFFINITY()

    # Create the root UI.
    ROOT_TITLE = "Affinity Circuit Sample"
    lRootUI = Tk()
    lRootUI.geometry("1000x600")
    lRootUI.resizable(1, 1)
    lRootUI.title(ROOT_TITLE)
    
    # Create the main canvas.
    lMainCanvas = uiAddCanvas(lRootUI)
    lCircuitUI = CircuitUI(lMainCanvas)

    # Implement menu handlers.
    def onMenuNew():
        # Setup the dlg...
        lDlg = Toplevel(lRootUI)
        def _onDlgOk():
            lDlg.destroy()
        lDlg.title("New Circuit...")
        lVarName = uiAddEdit(lDlg, "Name", pSetFocus=True, pOnReturn=lambda *args:_onDlgOk())[1]
        lVarWidth = uiAddEdit(lDlg, "Component Grid Width", pOnReturn=lambda *args:_onDlgOk())[1]; lVarWidth.set(5)
        lVarHeight = uiAddEdit(lDlg, "Component Grid Height", pOnReturn=lambda *args:_onDlgOk())[1]; lVarHeight.set(5)
        lVarDensity = uiAddEdit(lDlg, "Density of connections (average connections/component)", pOnReturn=lambda *args:_onDlgOk())[1]; lVarDensity.set(1.0)
        # TODO: Allow custom props and tags on the comps...
        Button(lDlg, text="OK", command=_onDlgOk).pack()
        lDlg.geometry("+%d+%d" % (lRootUI.winfo_rootx()+50, lRootUI.winfo_rooty()+50))
        lRootUI.wait_window(lDlg)
        # Handle the result.
        lNewName = lVarName.get()
        if lNewName and len(lNewName) > 0:
            lCircuit = Circuit(lNewName)
            lCircuit.addComponents(int(lVarWidth.get()), int(lVarHeight.get()), float(lVarDensity.get()))
            lCircuit = Circuit(lNewName) # TODO: remove this workaround when collection-related issues (eids etc.) are resolved...
            lCircuitUI.setCircuit(lCircuit)
            lRootUI.title("%s [%s]" % (ROOT_TITLE, lNewName))
    def onMenuLoad():
        # Setup the dlg...
        lCircuitList = PIN.loadPINs(lAffinity.qProto("SELECT * FROM \"http://localhost/afy/class/1.0/Circuit\";"))
        lNameList = [iC["http://localhost/afy/property/1.0/circuit/name"] for iC in lCircuitList]
        lDlg = Toplevel(lRootUI)
        lDlg.title("Load Circuit...")
        lList = uiAddList(lDlg)
        def _onDlgOk():
            lCurSel = lList.curselection()
            if len(lCurSel) > 0:
                lCircuit = Circuit(lNameList[int(lCurSel[0])])
                lCircuitUI.setCircuit(lCircuit)
                lRootUI.title("%s [%s]" % (ROOT_TITLE, lCircuit.name))
            lDlg.destroy()
        for iN in lNameList:
            lList.insert(END, iN)
        lList.bind("<Double-Button-1>", lambda *args:_onDlgOk())
        Button(lDlg, text="OK", command=_onDlgOk).pack()
        lDlg.geometry("+%d+%d" % (lRootUI.winfo_rootx()+50, lRootUI.winfo_rooty()+50))
        lRootUI.wait_window(lDlg)
    def onMenuRename():
        if not lCircuitUI.mCircuit:
            return
        # Setup the dlg...
        lDlg = Toplevel(lRootUI)
        def _onDlgOk():
            lDlg.destroy()
        lDlg.title("Rename Circuit...")
        lVarName = uiAddEdit(lDlg, "Name", pSetFocus=True, pOnReturn=lambda *args:_onDlgOk())[1]; lVarName.set(lCircuitUI.mCircuit.name)
        Button(lDlg, text="OK", command=_onDlgOk).pack()
        lDlg.geometry("+%d+%d" % (lRootUI.winfo_rootx()+50, lRootUI.winfo_rooty()+50))
        lRootUI.wait_window(lDlg)
        # Handle the result.
        lNewName = lVarName.get()
        if lNewName and len(lNewName) > 0:
            lCircuitUI.mCircuit.name = lNewName
            lRootUI.title("%s [%s]" % (ROOT_TITLE, lNewName))
    def onMenuFind():
        if not lCircuitUI.mCircuit:
            return
        lCircuitUI.unselect(pSoft=True)
        # Setup the dlg...
        # TODO: more fun stuff...
        lDlg = Toplevel(lRootUI)
        def _onDlgOk():
            lDlg.destroy()
        lDlg.title("Find Component...")
        lVarType1 = uiAddCombo(lDlg, "Type", Component.TYPES)[1]; lVarType1.set(Component.TYPES[0])
        lNil = "[nil]"
        lToTypes = Component.TYPES; lToTypes.append(lNil)
        lVarType2 = uiAddCombo(lDlg, "Connected-to Type", lToTypes)[1]; lVarType2.set(lNil)
        Button(lDlg, text="OK", command=_onDlgOk).pack()
        lDlg.geometry("+%d+%d" % (lRootUI.winfo_rootx()+50, lRootUI.winfo_rooty()+50))
        lRootUI.wait_window(lDlg)
        # Handle the result.
        lCandidates = []
        if lVarType2.get() != lNil:
            # Still not working... need to follow up with Mark (bug #183).
            #lCandidates = PIN.loadPINs(lAffinity.qProto( \
                #"SELECT * FROM (\"http://localhost/afy/class/1.0/Circuit/Component#bypos\" AS cp1 JOIN \"http://localhost/afy/class/1.0/Circuit\"('%s') AS c ON (cp1.afy:pinID = c.\"http://localhost/afy/property/1.0/circuit/components\")) JOIN \"http://localhost/afy/class/1.0/Circuit/Component#bypos\" AS cp2 ON (cp2.afy:pinID = cp1.\"http://localhost/afy/property/1.0/circuit/component/outputs\") WHERE (cp1.\"http://localhost/afy/property/1.0/circuit/component/type\"='%s' and cp2.\"http://localhost/afy/property/1.0/circuit/component/type\"='%s');" % \
                #(lCircuitUI.mCircuit.name, lVarType1.get(), lVarType2.get())))
            lCandidatesT = PIN.loadPINs(lAffinity.qProto( \
                "SELECT * FROM \"http://localhost/afy/class/1.0/Circuit/Component#bypos\" AS cp1 JOIN \"http://localhost/afy/class/1.0/Circuit/Component#bypos\" AS cp2 ON (cp2.afy:pinID = cp1.\"http://localhost/afy/property/1.0/circuit/component/outputs\") WHERE (cp1.\"http://localhost/afy/property/1.0/circuit/component/type\"='%s' and cp2.\"http://localhost/afy/property/1.0/circuit/component/type\"='%s');" % \
                (lVarType1.get(), lVarType2.get())))
            lCandidates = PIN.loadPINs(lAffinity.qProto( \
                "SELECT * FROM {%s} AS cp1 JOIN \"http://localhost/afy/class/1.0/Circuit\"('%s') AS c on (c.\"http://localhost/afy/property/1.0/circuit/components\" = cp1.afy:pinID);" % \
                (','.join([repr(_iCT.mPID) for _iCT in lCandidatesT]), lCircuitUI.mCircuit.name)))
        else:
            lCandidates = PIN.loadPINs(lAffinity.qProto( \
                "SELECT * FROM \"http://localhost/afy/class/1.0/Circuit/Component#bypos\" AS cp1 JOIN \"http://localhost/afy/class/1.0/Circuit\"('%s') AS c ON (cp1.afy:pinID = c.\"http://localhost/afy/property/1.0/circuit/components\") WHERE (cp1.\"http://localhost/afy/property/1.0/circuit/component/type\"='%s');" % \
                (lCircuitUI.mCircuit.name, lVarType1.get())))
        lCircuitUI.unselect(); lCircuitUI.unselect(pSoft=True)
        for iC in lCandidates:
            lCircuitUI.selectComponent(iC["http://localhost/afy/property/1.0/circuit/component/x"], iC["http://localhost/afy/property/1.0/circuit/component/y"], pCanvasCoords=True, pSoft=True)
    def onMenuEditComponent():
        if lCircuitUI.mSelected:
            # Setup the dlg...
            lDlg = Toplevel(lRootUI)
            def _onDlgOk():
                lDlg.destroy()
            lDlg.title("Edit Component...")
            lVarType = uiAddCombo(lDlg, "Type", Component.TYPES)[1]; lVarType.set(lCircuitUI.mSelected[0].ctype)
            Button(lDlg, text="OK", command=_onDlgOk).pack()
            lDlg.geometry("+%d+%d" % (lRootUI.winfo_rootx()+50, lRootUI.winfo_rooty()+50))
            lRootUI.wait_window(lDlg)
            # Handle the result.
            lNewType = lVarType.get()
            if lNewType and lNewType != lCircuitUI.mSelected[0].ctype:
                lCircuitUI.mSelected[0].mPin["http://localhost/afy/property/1.0/circuit/component/type"] = lNewType
                lCircuitUI.refresh()
            else:
                lCircuitUI.unselect()
    def onMenuConnectComponent():
        if lCircuitUI.mSelected:
            lTo = [iS[0] for iS in lCircuitUI.mSoftSelected]
            if lCircuitUI.mSelected[0] in lTo:
                lTo.remove(lCircuitUI.mSelected[0])
            if 0 != len(lTo):
                lCircuitUI.mCircuit.connectComponent(lCircuitUI.mSelected[0], lTo)
                lCircuitUI.refresh()
        lCircuitUI.unselect()
    def onMenuDisconnectComponent():
        if lCircuitUI.mSelected:
            lCircuitUI.mCircuit.disconnectComponent(lCircuitUI.mSelected[0])
            lCircuitUI.refresh()
    def onMenuDeleteComponent():
        if lCircuitUI.mSelected:
            lCircuitUI.mCircuit.deleteComponent(lCircuitUI.mSelected[0])
            lCircuitUI.refresh()

    # Configure the main menu.
    lMenuBar = Menu(lRootUI)
    lCircuitMenu = Menu(lMenuBar, tearoff=0)
    lCircuitMenu.add_command(label="New...", command=onMenuNew)
    lCircuitMenu.add_command(label="Load...", command=onMenuLoad)
    lCircuitMenu.add_command(label="Rename...", command=onMenuRename)
    lCircuitMenu.add_command(label="Find...", command=onMenuFind)
    lCircuitMenu.add_separator()
    lCircuitMenu.add_command(label="Exit", command=lRootUI.quit)
    lMenuBar.add_cascade(label="Circuit", menu=lCircuitMenu)
    lRootUI.config(menu=lMenuBar)

    # Configure the contextual menu.
    lCtxMenu = Menu(lRootUI, tearoff=0)
    lCtxMenu.add_command(label="Edit...", command=onMenuEditComponent)
    lCtxMenu.add_command(label="Connect To Blue", command=onMenuConnectComponent)
    lCtxMenu.add_command(label="Disconnect", command=onMenuDisconnectComponent)
    lCtxMenu.add_command(label="Delete", command=onMenuDeleteComponent)
    def displayCtxMenu(event):
        try:
            lCircuitUI.selectComponent(event.x, event.y)
            if lCircuitUI.mSelected:
                lCtxMenu.tk_popup(event.x_root + 20, event.y_root + 20, 0)
        finally:
            lCtxMenu.grab_release()
    lMainCanvas.bind("<Button-3>", displayCtxMenu)

    # Configure basic mouse selection.
    def softCtrlSelectComponent(event):
        lCircuitUI.selectComponent(event.x, event.y, pSoft=True)
    def softSelectComponent(event):
        lCircuitUI.unselect(); lCircuitUI.unselect(pSoft=True)
        softCtrlSelectComponent(event)
    lMainCanvas.bind("<Button-1>", softSelectComponent)
    lMainCanvas.bind("<Control-Button-1>", softCtrlSelectComponent)
    lMainCanvas.bind("<Shift-Button-1>", softCtrlSelectComponent)

    # Run.
    lAffinity.open()
    Circuit.declareAfyClasses()
    Circuit.describeModel()
    lRootUI.mainloop()
    lAffinity.close()
    print ("circuit1 exited normally.")
