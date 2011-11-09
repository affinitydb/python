#!/bin/sh
# I use this script on linux to setup everything I need for running the python test scripts resulting from code generation.
export PATH=$PATH:../protobuf/bin
export PYTHONPATH=$PYTHONPATH:../protobuf/python/:./ext/build/lib.linux-i686-2.6/
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:../kernel/lib/
