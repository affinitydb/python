SYSINFO=$(uname -a)
if echo $SYSINFO | grep -q 'i686'; then
    export CFLAGS="-march=i686"
elif echo $SYSINFO | grep -q 'x86_64'; then
    export CFLAGS="-march=nocona"
fi
python setup.py clean -a
python setup.py install