#!/usr/bin/env python2.6
"""This module implements a UI that leverages the information provided by the 'modeling'
module, and presents entity-relationship graphs of existing classes in the store. The initial
intent is to serve as simple demo material, help play with mvstore data modeling, and encourage
the use of the 'modeling' module. This might be converted into a more serious tool if it proves
useful and if time permits (e.g. on-the-fly modeling / class management, stat extraction
from existing raw data, multi-level modeling, visibility on emergent related classes, etc.)."""
# Note:
#   I'm also planning to use this as a preliminary lab for advanced features of
#   an eventual javascript store browser.
try:
    from tkinter import * # For UI.
except:
    from Tkinter import * # For UI.
from mvstore import * # For mvstore db access.
from uihelpers import * # For simple tkinter helpers.
import math
import modeling
import random

# TODO: tooltips for docstrings
# TODO: improve presentation logic (e.g. nearby entities)
# TODO: cleanup dead code
# TODO: more goodies (doc at the top of this file, plus allow to query instances of each entity with right-click, etc.)
# TODO: different views (e.g. native, uml, ...)

class ERDiagramUI(object):
    "Manage the main canvas, to represent a ER diagram of existing classes."
    ENTITY_WIDTH = 250 # Fixed width (in pixels) of boxes representing entities.
    ENTITY_HEIGHT = 150 # Fixed height (in pixels) of boxes representing entities.
    MARGIN = 30 # Fixed margin (in pixels) around entities.
    RELATION_COLORS = \
        ("#ff0000", "#dd0000", "#bb0000", "#990000", "#770000", "#550000", "#330000")
    def __init__(self, pCanvas):
        self.mCanvas = pCanvas # A Tkinter.Canvas.
        self.mData = None # A modeling.ERSchema.
        self.mSelected = None # A tuple (component, UI id).
        self.mSoftSelected = [] # A list of tuples (component, UI id).
    def setData(self):
        self.clear()
        self.mData = modeling.extractERSchema(pQNames={})
        print (self.mData)
        lMaxX = math.sqrt(self.mData.size)
        lX = lY = 0
        for iE in self.mData.mEntities.values():
            # Attribute a random, distinctive color to each relation.
            lNamedRel = []
            for iR in iE.mRelations:
                if iE == iR.mFkTarget:
                    iR.mColor = random.choice(ERDiagramUI.RELATION_COLORS)
                    lNamedRel.append(iR)
                    for iR2 in iR.mFk.mRelations:
                        if iE == iR2.mFk:
                            iR2.mColor = iR.mColor
                            break
            # Add mX and mY to self.mData's entities...
            iE.mX = lX
            iE.mY = lY
            # Produce the visuals.
            lRX = ERDiagramUI.MARGIN + lX * (ERDiagramUI.ENTITY_WIDTH + ERDiagramUI.MARGIN)
            lRY = ERDiagramUI.MARGIN + lY * (ERDiagramUI.ENTITY_HEIGHT + ERDiagramUI.MARGIN)
            self.mCanvas.create_rectangle(lRX, lRY, lRX + ERDiagramUI.ENTITY_WIDTH, lRY + ERDiagramUI.ENTITY_HEIGHT, tags=("component", "crect"), fill="#cccccc")
            self.mCanvas.create_text(lRX + 2, lRY + 2, text=iE.mClassURI, anchor=NW, tags=("component", "ctext"), font=("Helvetica", 8))
            i = 0
            for iA in iE.mAttributes.keys():
                self.mCanvas.create_text(lRX + 12, lRY + 2 + (i + 1) * 14, text=iA, anchor=NW, tags=("component", "ctext"), fill="#333333", font=("Helvetica", 8))
                i = i + 1
            for iR in lNamedRel:
                self.mCanvas.create_text(lRX + 12, lRY + 2 + (i + 1) * 14, text=iR.mURI, anchor=NW, tags=("component", "ctext"), fill=iR.mColor, font=("Helvetica", 8))
                i = i + 1
            lX = lX + 1
            if lX > lMaxX:
                lX = 0; lY = lY + 1
        for iE in self.mData.mEntities.values():
            for iR in iE.mRelations:
                if iE == iR.mFk:
                    self._createConnection(iE, iR.mFkTarget, iR.mColor)
        self.mCanvas.configure(scrollregion=(0, 0, 2 * ERDiagramUI.MARGIN + lMaxX * (ERDiagramUI.ENTITY_WIDTH + ERDiagramUI.MARGIN), 2 * ERDiagramUI.MARGIN + lMaxX * (ERDiagramUI.ENTITY_HEIGHT + ERDiagramUI.MARGIN)))
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
    def _createConnection(self, pE1, pE2, pColor):
        # Align the actual positions on a grid, to simplify things (to simplify handling of components displaced by the user).
        def __2phy(x=0, y=0):
            return ERDiagramUI.MARGIN + x * (ERDiagramUI.ENTITY_WIDTH + ERDiagramUI.MARGIN), ERDiagramUI.MARGIN + y * (ERDiagramUI.ENTITY_HEIGHT + ERDiagramUI.MARGIN)
        def __roc(): # Random Offset in Corner.
            return 2 + random.randrange(ERDiagramUI.MARGIN - 4)
        def __roe(): # Random Offset on Edge.
            _l = min(ERDiagramUI.ENTITY_WIDTH, ERDiagramUI.ENTITY_HEIGHT) - ERDiagramUI.MARGIN
            return random.randrange(_l) - _l/2
        # Create the segments.
        lPoints = []
        lX1p, lY1p = __2phy(pE1.mX, pE1.mY)
        lX2p, lY2p = __2phy(pE2.mX, pE2.mY)
        # Based on the direction, start either N,S,E,W; draw the crow's foot.
        lStartDirX = lStartDirY = 0
        if pE2.mX > pE1.mX:
            lStartDirX = 1
        elif pE2.mX < pE1.mX:
            lStartDirX = -1
        elif pE2.mY > pE1.mY:
            lStartDirY = 1
        else:
            lStartDirY = -1
        lPoints.append([lX1p + (0, __roe() + ERDiagramUI.ENTITY_WIDTH / 2, ERDiagramUI.ENTITY_WIDTH)[lStartDirX + 1], lY1p + (0, __roe() + ERDiagramUI.ENTITY_HEIGHT / 2, ERDiagramUI.ENTITY_HEIGHT)[lStartDirY + 1]])
        lPoints.append([lPoints[0][0] + lStartDirX * 5, lPoints[0][1] + lStartDirY * 5])
        lPoints.append([lPoints[0][0] - lStartDirY * 5, lPoints[0][1] - lStartDirX * 5])
        lPoints.append([lPoints[0][0] + lStartDirY * 5, lPoints[0][1] + lStartDirX * 5])
        lPoints.append([lPoints[1][0], lPoints[1][1]])
        if abs(pE2.mX - pE1.mX) + abs(pE2.mY - pE1.mY) > 1:
            lApproachX = abs(pE2.mX - pE1.mX) > 1
            lApproachY = abs(pE2.mY - pE1.mY) > 1
            # First, go to the closest corner near the initial entity.
            if pE2.mX > pE1.mX and pE2.mY > pE1.mY:
                lPoints.append([lX1p + ERDiagramUI.ENTITY_WIDTH + __roc(), lY1p + ERDiagramUI.ENTITY_HEIGHT + __roc()])
            elif pE2.mX > pE1.mX:
                lPoints.append([lX1p + ERDiagramUI.ENTITY_WIDTH + __roc(), lY1p - ERDiagramUI.MARGIN + __roc()])
            elif pE2.mY > pE1.mY:
                lPoints.append([lX1p - ERDiagramUI.MARGIN + __roc(), lY1p + ERDiagramUI.ENTITY_HEIGHT + __roc()])
            else:
                lPoints.append([lX1p - ERDiagramUI.MARGIN + __roc(), lY1p - ERDiagramUI.MARGIN + __roc()])
            # Second, approach the terminal entity in X (if needed).
            if lApproachX:
                if pE2.mX > pE1.mX:
                    lPoints.append([lX2p - ERDiagramUI.MARGIN + __roc(), lPoints[-1][1]])
                else:
                    lPoints.append([lX2p + ERDiagramUI.ENTITY_WIDTH + __roc(), lPoints[-1][1]])
            # Third, approach the terminal entity in Y (if needed).
            if lApproachY:
                if pE2.mY > pE1.mY:
                    lPoints.append([lPoints[-1][0], lY2p - ERDiagramUI.MARGIN + __roc()])
                else:
                    lPoints.append([lPoints[-1][0], lY2p + ERDiagramUI.ENTITY_HEIGHT + __roc()])
        # Finally, link to the entity.
        lPoints.append([lX2p + (0, __roe() + ERDiagramUI.ENTITY_WIDTH / 2, ERDiagramUI.ENTITY_WIDTH)[-lStartDirX + 1], lY2p + (0, __roe() + ERDiagramUI.ENTITY_HEIGHT / 2, ERDiagramUI.ENTITY_HEIGHT)[-lStartDirY + 1]])
        # Proceed.
        for iP in xrange(len(lPoints)):
            if iP == 0:
                continue
            self.mCanvas.create_line(lPoints[iP - 1][0], lPoints[iP - 1][1], lPoints[iP][0], lPoints[iP][1], fill=pColor, tags="connection")
        
if __name__ == '__main__':
    # Have a db connection.
    lMvStore = MVSTORE()

    # Create the root UI.
    ROOT_TITLE = "MvStore ER Diagram Sample"
    lRootUI = Tk()
    lRootUI.geometry("1000x600")
    lRootUI.resizable(1, 1)
    lRootUI.title(ROOT_TITLE)
    
    # Create the main canvas.
    lMainCanvas = uiAddCanvas(lRootUI)
    lERDiagramUI = ERDiagramUI(lMainCanvas)

    # Implement menu handlers.
    def onMenuFind():
        if not lERDiagramUI.mCircuit:
            return
        lERDiagramUI.unselect(pSoft=True)
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
            lCandidates = PIN.loadPINs(lMvStore.qProto( \
                "SELECT * FROM (components_bypos AS cp1 JOIN circuits('%s') AS c ON (cp1.afy:pinID = c.components)) INTERSECT SELECT * FROM components_bypos WHERE (cp2.afy:pinID = cp1.component_outputs AND cp1.component_type='%s' AND cp2.component_type='%s');" % \
                (lERDiagramUI.mCircuit.name, lVarType1.get(), lVarType2.get())))
        else:
            lCandidates = PIN.loadPINs(lMvStore.qProto( \
                "SELECT * FROM components_bypos AS cp1 JOIN circuits('%s') AS c ON (cp1.afy:pinID = c.components) WHERE (cp1.component_type='%s');" % \
                (lERDiagramUI.mCircuit.name, lVarType1.get())))
        lERDiagramUI.unselect(); lERDiagramUI.unselect(pSoft=True)
        for iC in lCandidates:
            lERDiagramUI.selectComponent(iC["component_x"], iC["component_y"], pCanvasCoords=True, pSoft=True)
    def onMenuEditComponent():
        if lERDiagramUI.mSelected:
            # Setup the dlg...
            lDlg = Toplevel(lRootUI)
            def _onDlgOk():
                lDlg.destroy()
            lDlg.title("Edit Component...")
            lVarType = uiAddCombo(lDlg, "Type", Component.TYPES)[1]; lVarType.set(lERDiagramUI.mSelected[0].ctype)
            Button(lDlg, text="OK", command=_onDlgOk).pack()
            lDlg.geometry("+%d+%d" % (lRootUI.winfo_rootx()+50, lRootUI.winfo_rooty()+50))
            lRootUI.wait_window(lDlg)
            # Handle the result.
            lNewType = lVarType.get()
            if lNewType and lNewType != lERDiagramUI.mSelected[0].ctype:
                lERDiagramUI.mSelected[0].mPin["component_type"] = lNewType
                lERDiagramUI.refresh()
            else:
                lERDiagramUI.unselect()

    # Configure the main menu.
    lMenuBar = Menu(lRootUI)
    lMainMenu = Menu(lMenuBar, tearoff=0)
    lMainMenu.add_command(label="Find...", command=onMenuFind)
    lMainMenu.add_separator()
    lMainMenu.add_command(label="Exit", command=lRootUI.quit)
    lMenuBar.add_cascade(label="Diagram", menu=lMainMenu)
    lRootUI.config(menu=lMenuBar)

    # Configure the contextual menu.
    lCtxMenu = Menu(lRootUI, tearoff=0)
    lCtxMenu.add_command(label="Edit...", command=onMenuEditComponent)
    def displayCtxMenu(event):
        try:
            lERDiagramUI.selectComponent(event.x, event.y)
            if lERDiagramUI.mSelected:
                lCtxMenu.tk_popup(event.x_root + 20, event.y_root + 20, 0)
        finally:
            lCtxMenu.grab_release()
    lMainCanvas.bind("<Button-3>", displayCtxMenu)

    # Configure basic mouse selection.
    def softCtrlSelectComponent(event):
        lERDiagramUI.selectComponent(event.x, event.y, pSoft=True)
    def softSelectComponent(event):
        lERDiagramUI.unselect(); lERDiagramUI.unselect(pSoft=True)
        softCtrlSelectComponent(event)
    lMainCanvas.bind("<Button-1>", softSelectComponent)
    lMainCanvas.bind("<Control-Button-1>", softCtrlSelectComponent)
    lMainCanvas.bind("<Shift-Button-1>", softCtrlSelectComponent)

    # Run.
    lMvStore.open()
    modeling.initialize()
    lERDiagramUI.setData()
    lRootUI.mainloop()
    lMvStore.close()
    print ("erdiagram exited normally.")
