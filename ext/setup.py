from distutils.core import setup, Extension
import platform
if "Darwin" in platform.system():
    module = Extension('affinityinproc', sources = ['pyaffinitymodule.cpp', '../../server/src/storenotifier.cpp'], include_dirs=['../../kernel/include', '../../server/src'], libraries=['affinity'], library_dirs=['../../kernel/lib'], extra_compile_args=['-DDarwin'])
else:
    module = Extension('affinityinproc', sources = ['pyaffinitymodule.cpp', '../../server/src/storenotifier.cpp'], include_dirs=['../../kernel/include', '../../server/src'], libraries=['affinity'], library_dirs=['../../kernel/lib'])
setup(name = 'affinityinproc', version = '1.0', description = 'Small python extension to emulate the Affinity server, presenting a similar interface but using Affinity in-process.', ext_modules = [module])
