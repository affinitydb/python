REM I use this script on windows to setup everything I need for running the python test scripts resulting from code generation.
REM Note: This will need to be improved to handle all possible combinations (Debug/Release, different python versions etc.).

set PYTHONPATH=%PYTHONPATH%;..\protobuf\python\;.\ext\build\lib.win32-2.6\
set PATH=%PATH%;..\kernel\lib\Debug\;..\protobuf\bin\Debug\
