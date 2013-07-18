#!/usr/bin/env python2.6
# Copyright (c) 2004-2013 GoPivotal, Inc. All Rights Reserved.
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
"""This module implements simple tkinter UI helpers used by some samples."""
try:
    from tkinter import * # For UI.
except:
    from Tkinter import * # For UI.

def onAnyDoNothing():
    pass

def uiAddEdit(pRoot, pLabel, pSetFocus=False, pOnReturn=lambda *args:onAnyDoNothing()):
    "Package together an Entry control, a String variable, and a Label (within a Frame); return the control and its variable."
    lVar = StringVar()
    lVar.set("")
    lFrame = Frame(pRoot)
    lLabel = Label(lFrame, text=pLabel)
    lLabel.pack(side=LEFT)
    lEntry = Entry(lFrame, textvariable=lVar)
    if pSetFocus:
        lEntry.focus_set()
    lEntry.pack(side=LEFT, fill=BOTH, expand=1)
    lEntry.bind("<KeyPress-Return>", pOnReturn)
    lFrame.pack(fill=BOTH)
    return (lEntry, lVar)

def uiAddCombo(pRoot, pLabel, pValues, pSetFocus=False, pOnReturn=lambda *args:onAnyDoNothing()):
    "Package together a Combobox control, a String variable, and a Label (within a Frame); return the control and its variable."
    lVar = StringVar()
    lVar.set("")
    lFrame = Frame(pRoot)
    lLabel = Label(lFrame, text=pLabel)
    lLabel.pack(side=LEFT)
    lCombo = OptionMenu(lFrame, lVar, *pValues)
    if pSetFocus:
        lCombo.focus_set()
    lCombo.pack(side=LEFT, fill=BOTH, expand=1)
    lCombo.bind("<KeyPress-Return>", pOnReturn)
    lFrame.pack(fill=BOTH)
    return (lCombo, lVar)

def uiAddList(pRoot, pOnClick=lambda *args:onAnyDoNothing()):
    "Package, configure and return a Listbox control with scrollbars."
    lFrame = Frame(pRoot)
    lFrame.grid(row=0, column=0, sticky=N+S+E+W)
    lList = Listbox(lFrame, selectmode=SINGLE)
    lList.bind("<ButtonRelease>", pOnClick)
    lXScroll = Scrollbar(lFrame, orient=HORIZONTAL)
    lXScroll.grid(row=1, column=0, sticky=E+W)
    lXScroll["command"] = lList.xview
    lYScroll = Scrollbar(lFrame, orient=VERTICAL)
    lYScroll.grid(row=0, column=1, sticky=N+S)
    lYScroll["command"] = lList.yview
    lList.config(xscrollcommand=lXScroll.set, yscrollcommand=lYScroll.set)
    lList.grid(row=0, column=0, sticky=N+S+E+W)
    lList.config(width=50, height=10)
    lFrame.pack(fill=BOTH)
    return lList

def uiAddCanvas(pRoot):
    "Package, configure and return a Canvas control with scrollbars."
    lFrame = Frame(pRoot)
    lFrame.grid(row=0, column=0, sticky=N+S+E+W)
    lCanvas = Canvas(lFrame, scrollregion=(0,0,100,100))
    lXScroll = Scrollbar(lFrame, orient=HORIZONTAL)
    lXScroll.grid(row=1, column=0, sticky=E+W)
    lXScroll["command"] = lCanvas.xview
    lYScroll = Scrollbar(lFrame, orient=VERTICAL)
    lYScroll.grid(row=0, column=1, sticky=N+S)
    lYScroll["command"] = lCanvas.yview
    lCanvas.config(xscrollcommand=lXScroll.set, yscrollcommand=lYScroll.set)
    lCanvas.grid(row=0, column=0, sticky=N+S+E+W)
    lFrame.grid_columnconfigure(0, weight=1)
    lFrame.grid_rowconfigure(0, weight=1)
    lFrame.pack(fill=BOTH, expand=1)
    return lCanvas
