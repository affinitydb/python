This python extension module emulates the store server, by exposing
entry points equivalent to the server's /db http services.
This allows to run tests with Affinity inproc. It's convenient
for quick experimentation.

To this date I have only tested this with python 2.6.

To build the module, it suffices to follow the standard
distutils procedure:

  python setup.py build

On linux
--------
I had a pre-configured python 2.6.  I didn't encounter any issue.

(1)  Install python-dev version:
su root
yum install python26-devel.i386

(2) install protobuf python module for python environment
cd protobuf/python
python setup.py install

(3) If you running in server mode, make sure no proxy is set:
export http_proxy=

On windows
----------
I installed python 2.6 from this link:

  http://www.python.org/download/releases/2.6.6/

I had to follow the directives below, to bind my visual studio 10
to distutils, to be able to build my c++ extension:

  http://nukeit.org/compile-python-2-7-packages-with-visual-studio-2010-express/

I also had to install this:

  http://pypi.python.org/pypi/setuptools#downloads

Finally, I had to append 'Debug' to a path in setup.py (../../kernel/lib/Debug),
because our kernel c++ build currently does not follow the exact same
path conventions on windows and linux.


##Tao's comments 

###Below is how to build and install python lib. (Max, please correct me if I mistake.)

1.	Follow the tips above given by Max:

	a.	Install python2.6.6. (Python 3 does not work for protobuf). Append "C:/Python26" to environmental variables "PATH".
	
	b.	Download and install stepuptools. I choose ["setuptools-0.6c11.win32-py2.6.exe"](http://pypi.python.org/packages/2.6/s/setuptools/setuptools-0.6c11.win32-py2.6.exe#md5=1509752c3c2e64b5d0f9589aafe053dc).
	
	c.	Bind my visual studio 10 to distutils, see the link above. Notice that for our Python 2.6.6, the line# is not exactly the same.
	
		(1)	In "C:/Python26/Lib/distutils/msvc9compiler.py" at line 232 (in function "find_vcvarsall"). Replace:
		
			> toolskey = "VS%0.f0COMNTOOLS" % version
		
			with line
		
			> toolskey = "VS100COMNTOOLS"
		
		(2)	At line 644 (in class MSVCCompiler, function link), add line:
		
			> ld_args.append('/MANIFEST')
		
			before line
		
			> self.mkpath(os.path.dirname(output_filename))
			
		(3) Set an environmental variable
		
			> Name = VS100COMNTOOLS
			
			> Value = C:/Program Files/Microsoft Visual Studio 10.0/Common7/Tools/
	
2.	First, we need to build and install protobuf. See readme files in protobuf/, protobuf/vsprojects, protobuf/python/.

	a.	Goto "protobuf/vsprojects", open and build the VS solution. Copy "protobuf/vsprojects/Debug/<font style="color:red">protoc.exe</font>" to "protobuf/python"
	
	b.	Goto "protobuf/python", run "python -V", "protoc --version", "python setup.py test" should have no problems. Then run:
	
		> python setup.py install

	c.	Copy dir "protobuf/python/google" to python lib dir "python/".
	
3.	In direcory python/ext: (for windows):

	a.	The second line of setup.py should be like this.
   
	> module = Extension('affinityinproc', <font style="color:red">define_macros = [('WIN32', '1')]</font>, sources = ['pyaffinitymodule.cpp', '../../server/src/storenotifier.cpp'], include_dirs=['../../kernel/include', '../../server/src'], libraries=['affinity'], library_dirs=['../../kernel/lib/<font style="color:red">Debug</font>'])
	
	b.	Python module (<font style="color:red">affinityinproc.pyd</font>) will be generated in "python/ext/build/lib.win32-2.6". Put it in python path. If this module is not found, python lib will use server to access Affinity.
	
4.	"setup.cmd" in dir "python/" need to be run every time, if the paths are not set into environmental variables.

5.	Then we can run tests and samples and use python libarary.
