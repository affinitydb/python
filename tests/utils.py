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
""" utility module for python testing framework """
import math 

def timeToString(cost):
    hours = int(cost/(60*60))
    minutes = int(cost/60 - hours*60)
    remain = cost%60
    time = ""
    if (remain > 0):
        time = "%.3f seconds" % remain
    if (minutes > 0):
        if (minutes > 1):
            time = "%d minutes " % minutes + time
        elif (minutes == 1):
            time = "%d minute " % minutes + time
    if (hours > 0):
        if (hours > 1):
            time = "%d hours " % hours + time
        elif (hours == 1):
            time = "%d hour " % hours + time
    return time
    
def norm(v,p):
    # return the result of norm()
    # reference: http://www.mathworks.cn/cn/help/symbolic/norm.html
    result = 0
    if p == 1:
        for element in v:
            result += math.fabs(element)
    elif p == 2 or p == 0:
        # norm(v,2) == norm(v) (norm(v,0) stands for norm(v) here)
        for element in v:
            result += math.pow(element,2)
        result = math.sqrt(result)
    elif p > 2:
        for element in v:
            result += math.pow(math.fabs(element),p)
        result = math.pow(result,1.0/p)
    else:
        assert 0
    
    return result
