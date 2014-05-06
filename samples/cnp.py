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
"""This module is a first trivial example of a physical simulation (the famous "cart and pole"
scenario, aka "inverted pendulum"), with a PID controller used to move the cart such as to
keep the inverted pendulum in a vertical position. The intent was to get acquainted with
the basic concepts, and experiment with a simple test platform that integrates inproc pathSQL
processing and a reasonably fast rendering environment."""
try:
    from tkinter import * # For UI.
except:
    from Tkinter import * # For UI.
from affinity import * # For Affinity db access.
from uihelpers import * # For simple tkinter helpers.
import math
import modeling
import random

class EvaluationPy(object):
    "Pure-python reference inproc evaluation of the simulation and PID control."
    class Simulation(object):
        "The physical simulation."
        def __init__(self):
            self.I = .005
            self.M = .5
            self.m = .2
            self.l = 1.0
            self.b = .1
            self.Beta1 = self.I + self.m * math.pow(self.l, 2)
            self.Beta2 = self.I * (self.M + self.m) + self.M * self.m * math.pow(self.l, 2)
            self.cart_x = 0
            self.cart_speed = 0
            self.angle = 0
            self.aspeed = 0
            self.t = 0
            self.tstep = 0.02
            self.F = -5.5
        def calculate(self):
            if self.angle <= -math.pi * 0.5 or self.angle >= math.pi * 0.5:
                return
            lPrevSpeed = self.cart_speed
            lPrevAngle = self.angle
            self.cart_x = EvaluationPy.rungeKutta(lambda _pT, _pV: self.cart_speed, self.cart_x, self.t, self.tstep)
            self.cart_speed = EvaluationPy.rungeKutta(lambda _pT, _pV: ((-self.Beta1 * self.b * _pV) + (math.pow(self.m * self.l, 2) * 9.80665 * self.angle) + (self.Beta1 * self.F)) / self.Beta2, self.cart_speed, self.t, self.tstep)
            self.angle = EvaluationPy.rungeKutta(lambda _pT, _pV: self.aspeed, self.angle, self.t, self.tstep)
            self.aspeed = EvaluationPy.rungeKutta(lambda _pT, _pV: ((-self.m * self.l * self.b * lPrevSpeed) + (self.m * 9.80665 * self.l * (self.M + self.m) * lPrevAngle) + (self.m * self.l * self.F)) / self.Beta2, self.aspeed, self.t, self.tstep)
            self.t += self.tstep
    class PID(object):
        "The PID control to track the error on the angle between the pole and the cart."
        def __init__(self):
            self.e = 0
            self.i = 0
            self.d = 0
            self.Kp = 1.0
            self.Ki = 1.0
            self.Kd = 1.0
        def calculate(self, pSimulation):
            lPrevError = self.e
            self.e = 0 - pSimulation.angle
            self.i += self.e * pSimulation.tstep
            self.d = (self.e - lPrevError) / pSimulation.tstep
            return self.Kp * self.e + self.Ki * self.i + self.Kd * self.d # angle correction
    def __init__(self):
        self.mSimulation = EvaluationPy.Simulation()
        self.mPID = EvaluationPy.PID()
    def calculate(self):
        # Evaluate the simulation.
        self.mSimulation.calculate()
        # Let the PID evaluate the force to be applied on the cart at next iteration.
        if True:
            forceFromDeltaAngle = lambda _pDa: 8 * _pDa
            self.mSimulation.F = forceFromDeltaAngle(self.mPID.calculate(self.mSimulation))
        # Return {position, angle}.
        return self.mSimulation.cart_x, self.mSimulation.angle
    @staticmethod
    def rungeKutta(pFunc, pV, pT, pDt):
        "Compute the next value according to pFunc (the ODE); pV is the initial value, pT is the initial time, and pDt is the time increment."
        lHalfDt = 0.5 * pDt
        a = pFunc(pT, pV)
        b = pFunc(pT + lHalfDt, pV + lHalfDt * a)
        c = pFunc(pT + lHalfDt, pV + lHalfDt * b)
        d = pFunc(pT + pDt, pV + pDt * c)
        return (pV + pDt * 0.16666666 * (a + 2*b + 2*c + d))

class EvaluationPathSQL(object):
    "Pure-pathSQL evaluation of the simulation and PID control."
    class Simulation(object):
        "The physical simulation."
        def __init__(self, pAffinity):
            self.mAffinity = pAffinity
            self.angle = 0
            # Note: I tried to implement the evaluators with $(...), but could not figure how to invoke this successfully,
            #       such as to take advantage of a context where fixed parameters would be defined externally (e.g. #thecnp).
            # Note: I was forced to do a bunch of gymnastics with :LAST and also lE1r intermediate result... log as bugs...
            l0 = (\
                "INSERT afy:objectID='thecnp', cnp:I=0.005, cnp:M=0.5, cnp:m=0.2, cnp:l=1.0, cnp:b=0.1, cnp:tstep=0.02",
                "UPDATE #thecnp SET cnp:beta1=(cnp:I + cnp:m * POWER(cnp:l, 2))",
                "UPDATE #thecnp SET cnp:beta2=(cnp:I * (cnp:M + cnp:m) + cnp:M * cnp:m * POWER(cnp:l, 2))",
                "UPDATE #thecnp SET cnp:cart_x=0, cnp:cart_speed=0, cnp:angle=0, cnp:aspeed=0, cnp:F=-5.5, cnp:t=0, cnp:prev_speed=0, cnp:prev_angle=0",
                "CREATE CLASS cnp:eval_x AS SELECT * WHERE cnp:eval='x' SET afy:onEnter=${UPDATE @self SET cnp:r=(SELECT cnp:cart_speed FROM #thecnp)}",
                "CREATE CLASS cnp:eval_speed AS SELECT * WHERE cnp:eval='speed' SET afy:onEnter={${UPDATE #thecnp SET cnp:_v=@self.cnp:v}, ${UPDATE @self SET cnp:r=(SELECT (((-cnp:beta1 * cnp:b * cnp:_v) + (POWER(cnp:m * cnp:l, 2) * 9.80665 * cnp:angle) + (cnp:beta1 * cnp:F)) / cnp:beta2) FROM #thecnp)}}", # n.b. can't mix in @self.cnp:v (neither with JOIN)...
                "CREATE CLASS cnp:eval_angle AS SELECT * WHERE cnp:eval='angle' SET afy:onEnter=${UPDATE @self SET cnp:r=(SELECT cnp:aspeed FROM #thecnp)}",
                "CREATE CLASS cnp:eval_aspeed AS SELECT * WHERE cnp:eval='aspeed' SET afy:onEnter=${UPDATE @self SET cnp:r=(SELECT (((-cnp:m * cnp:l * cnp:b * cnp:prev_speed[:LAST]) + (cnp:m * 9.80665 * cnp:l * (cnp:M + cnp:m) * cnp:prev_angle[:LAST]) + (cnp:m * cnp:l * cnp:F)) / cnp:beta2) FROM #thecnp)}",
                "CREATE CLASS cnp:runge_kutta AS SELECT * WHERE EXISTS(cnp:runge_kutta_type) SET afy:onEnter={\n\
                    ${UPDATE @self SET lHalfDt=(0.5 * @self.cnp:dt)},\n\
                    ${UPDATE @self SET lAe=(INSERT cnp:eval=@self.cnp:runge_kutta_type, cnp:t=@self.cnp:t, cnp:v=@self.cnp:v)},\n\
                    ${UPDATE @self SET lA=(SELECT cnp:r FROM @self.lAe)},\n\
                    ${UPDATE @self SET lBe=(INSERT cnp:eval=@self.cnp:runge_kutta_type, cnp:t=@self.cnp:t + @self.lHalfDt, cnp:v=@self.cnp:v + @self.lHalfDt * @self.lA[:LAST])},\n\
                    ${UPDATE @self SET lB=(SELECT cnp:r FROM @self.lBe)},\n\
                    ${UPDATE @self SET lCe=(INSERT cnp:eval=@self.cnp:runge_kutta_type, cnp:t=@self.cnp:t + @self.lHalfDt, cnp:v=@self.cnp:v + @self.lHalfDt * @self.lB[:LAST])},\n\
                    ${UPDATE @self SET lC=(SELECT cnp:r FROM @self.lCe)},\n\
                    ${UPDATE @self SET lDe=(INSERT cnp:eval=@self.cnp:runge_kutta_type, cnp:t=@self.cnp:t + @self.cnp:dt, cnp:v=@self.cnp:v + @self.cnp:dt * @self.lC[:LAST])},\n\
                    ${UPDATE @self SET lD=(SELECT cnp:r FROM @self.lDe)},\n\
                    ${UPDATE @self SET cnp:result=cnp:v + cnp:dt * 0.16666666 * (lA[:LAST] + 2*lB[:LAST] + 2*lC[:LAST] + lD[:LAST])}}",
                "CREATE CLASS cnp:simulation_step AS SELECT * WHERE EXISTS(cnp:simulation_step_call) SET afy:onEnter={\n\
                    ${UPDATE #thecnp SET cnp:prev_speed=(SELECT cnp:cart_speed FROM #thecnp)},\n\
                    ${UPDATE #thecnp SET cnp:prev_angle=(SELECT cnp:angle FROM #thecnp)},\n\
                    ${UPDATE @self SET lT=(SELECT cnp:t FROM #thecnp)},\n\
                    ${UPDATE @self SET lDt=(SELECT cnp:tstep FROM #thecnp)},\n\
                    ${UPDATE @self SET lX=(SELECT cnp:cart_x FROM #thecnp)},\n\
                    ${UPDATE @self SET lE1=(INSERT cnp:runge_kutta_type='x', cnp:v=@self.lX[:LAST], cnp:t=@self.lT[:LAST], cnp:dt=@self.lDt[:LAST])},\n\
                    ${UPDATE @self SET lE1r=(SELECT cnp:result FROM @self.lE1)},\n\
                    ${UPDATE #thecnp SET cnp:cart_x=(SELECT lE1r[:LAST] FROM @self)},\n\
                    ${UPDATE @self SET lSpeed=(SELECT cnp:cart_speed FROM #thecnp)},\n\
                    ${UPDATE @self SET lE2=(INSERT cnp:runge_kutta_type='speed', cnp:v=@self.lSpeed[:LAST], cnp:t=@self.lT[:LAST], cnp:dt=@self.lDt[:LAST])},\n\
                    ${UPDATE @self SET lE2r=(SELECT cnp:result FROM @self.lE2)},\n\
                    ${UPDATE #thecnp SET cnp:cart_speed=(SELECT lE2r[:LAST] FROM @self)},\n\
                    ${UPDATE @self SET lAngle=(SELECT cnp:angle FROM #thecnp)},\n\
                    ${UPDATE @self SET lE3=(INSERT cnp:runge_kutta_type='angle', cnp:v=@self.lAngle[:LAST], cnp:t=@self.lT[:LAST], cnp:dt=@self.lDt[:LAST])},\n\
                    ${UPDATE @self SET lE3r=(SELECT cnp:result FROM @self.lE3)},\n\
                    ${UPDATE #thecnp SET cnp:angle=(SELECT lE3r[:LAST] FROM @self)},\n\
                    ${UPDATE @self SET lAspeed=(SELECT cnp:aspeed FROM #thecnp)},\n\
                    ${UPDATE @self SET lE4=(INSERT cnp:runge_kutta_type='aspeed', cnp:v=@self.lAspeed[:LAST], cnp:t=@self.lT[:LAST], cnp:dt=@self.lDt[:LAST])},\n\
                    ${UPDATE @self SET lE4r=(SELECT cnp:result FROM @self.lE4)},\n\
                    ${UPDATE #thecnp SET cnp:aspeed=(SELECT lE4r[:LAST] FROM @self)},\n\
                    ${UPDATE #thecnp SET cnp:t+=cnp:tstep}}")
                #"CREATE TIMER cnp:simulation INTERVAL '00:00:00.05' AS INSERT cnp:simulation_step_call=1")
            l1 = ("UPDATE #thecnp SET cnp:cart_x=0, cnp:cart_speed=0, cnp:angle=0, cnp:aspeed=0, cnp:F=-5.5, cnp:t=0, cnp:prev_speed=0, cnp:prev_angle=0",)
            self.mAffinity.setPrefix("cnp", "http://example/cart_and_pole")
            lCode = l0
            if 0 < self.mAffinity.qCount("SELECT * FROM afy:NamedObjects WHERE CONTAINS(afy:objectID, 'thecnp')"):
                lCode = l1
            for iC in lCode:
                self.mAffinity.q(iC)
        def calculate(self):
            if self.angle <= -math.pi * 0.5 or self.angle >= math.pi * 0.5:
                return
            self.mAffinity.q("INSERT cnp:simulation_step_call=1")
            lO = PIN.loadPINs(self.mAffinity.qProto("SELECT * FROM #thecnp"))[0] # cnp:cart_x, cnp:angle FROM #thecnp"))[0]
            self.angle = lO["http://example/cart_and_pole/angle"]
            self.cart_x = lO["http://example/cart_and_pole/cart_x"]
    class PID(object):
        "The PID control to track the error on the angle between the pole and the cart."
        def __init__(self, pAffinity):
            self.mAffinity = pAffinity
            l0 = (\
                "INSERT afy:objectID='thepid', cnp:e=0, cnp:i=0, cnp:d=0, cnp:Kp=1.0, cnp:Ki=1.0, cnp:Kd=1.0",
                "CREATE CLASS cnp:eval_pid AS SELECT * WHERE EXISTS(cnp:eval_pid_call) SET afy:onEnter={\n\
                    ${UPDATE @self SET lPrevError=(SELECT cnp:e FROM #thepid)},\n\
                    ${UPDATE @self SET lTstep=(SELECT cnp:tstep FROM #thecnp)},\n\
                    ${UPDATE @self SET lE=(SELECT cnp:angle FROM #thecnp)},\n\
                    ${UPDATE #thepid SET cnp:e=0 - @self.lE[:LAST]},\n\
                    ${UPDATE #thepid SET cnp:i=cnp:e * @self.lTstep[:LAST]},\n\
                    ${UPDATE #thepid SET cnp:d=(cnp:e - @self.lPrevError[:LAST]) / @self.lTstep[:LAST]},\n\
                    ${UPDATE #thepid SET cnp:output=(cnp:Kp * cnp:e + cnp:Ki * cnp:i + cnp:Kd * cnp:d)},\n\
                    ${UPDATE @self SET lO=(SELECT cnp:output FROM #thepid)},\n\
                    ${UPDATE #thecnp SET cnp:F=8.0 * @self.lO[:LAST]}}")
            l1 = ("UPDATE #thepid SET cnp:e=0, cnp:i=0, cnp:d=0",)
            self.mAffinity.setPrefix("cnp", "http://example/cart_and_pole")
            lCode = l0
            if 0 < self.mAffinity.qCount("SELECT * FROM afy:NamedObjects WHERE CONTAINS(afy:objectID, 'thepid')"):
                lCode = l1
            for iC in lCode:
                self.mAffinity.q(iC)
        def calculate(self):
            self.mAffinity.q("INSERT cnp:eval_pid_call=1")
    def __init__(self, pAffinity):
        self.mSimulation = EvaluationPathSQL.Simulation(pAffinity)
        self.mPID = EvaluationPathSQL.PID(pAffinity)
    def calculate(self):
        self.mSimulation.calculate()
        self.mPID.calculate()
        # print "result: %s, %s" % (self.mSimulation.cart_x, self.mSimulation.angle)
        return self.mSimulation.cart_x, self.mSimulation.angle

class CartAndPole(object):
    "Evaluate and render the simulation."
    POLE_LENGTH = 200 # Fixed length (in pixels) of the pole.
    COLORS = ("#000000", "#dd0000")
    def __init__(self, pCanvas, pEvalFactory):
        self.mCanvas = pCanvas # A Tkinter.Canvas.
        self.mCanvas.create_line(500, 100, 500, 100 + CartAndPole.POLE_LENGTH, fill=CartAndPole.COLORS[0], tags="pole")
        self.mEvalFactory = pEvalFactory
        self.mEval = self.mEvalFactory()
    def restart(self):
        self.mEval = self.mEvalFactory()
    def onTimer(self):
        lCartX, lAngle = self.mEval.calculate()
        lX2 = lCartX * CartAndPole.POLE_LENGTH
        lY2 = 0
        lX1 = lX2 - math.sin(math.pi - lAngle) * CartAndPole.POLE_LENGTH
        lY1 = math.cos(math.pi - lAngle) * CartAndPole.POLE_LENGTH
        self.mCanvas.coords("pole", 500 + lX1, 300 + lY1, 500 + lX2, 300 + lY2)
        self.mCanvas.winfo_toplevel().after(50, self.onTimer)

if __name__ == '__main__':
    # Have a db connection.
    lAffinity = AFFINITY()
    lAffinity.open()

    # Create the root UI.
    ROOT_TITLE = "Simple PID Exercise"
    lRootUI = Tk()
    lRootUI.geometry("1000x600")
    lRootUI.resizable(1, 1)
    lRootUI.title(ROOT_TITLE)

    # Create the main canvas.
    lMainCanvas = uiAddCanvas(lRootUI)
    #lCnp = CartAndPole(lMainCanvas, lambda:EvaluationPy())
    lCnp = CartAndPole(lMainCanvas, lambda:EvaluationPathSQL(lAffinity))

    # Implement menu handlers.
    def onMenuRestart():
        lCnp.restart()

    # Configure the main menu.
    lMenuBar = Menu(lRootUI)
    lMainMenu = Menu(lMenuBar, tearoff=0)
    lMainMenu.add_command(label="Restart", command=onMenuRestart)
    lMainMenu.add_command(label="Exit", command=lRootUI.quit)
    lMenuBar.add_cascade(label="Options", menu=lMainMenu)
    lRootUI.config(menu=lMenuBar)

    # Configure basic mouse selection.
    def softCtrlSelectComponent(event):
        return
    def softSelectComponent(event):
        return
    lMainCanvas.bind("<Button-1>", softSelectComponent)
    lMainCanvas.bind("<Control-Button-1>", softCtrlSelectComponent)
    lMainCanvas.bind("<Shift-Button-1>", softCtrlSelectComponent)

    # Run.
    modeling.initialize()
    lCnp.onTimer()
    lRootUI.mainloop()
    lAffinity.close()
    print ("cnp exited normally.")
