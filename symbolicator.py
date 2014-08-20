#!/usr/bin/env python2

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from bisect import bisect
from collections import namedtuple
from datetime import datetime
import ftplib
import os
import subprocess
import zipfile

Symbol = namedtuple("Symbol", "library function source line")

class BreakpadSymbolFile:
    '''
    A symbolicator targetting Breakpad-format symbol files.
    '''
    Func = namedtuple("Func", "start end name offset")

    def __init__(self, filename):
        '''
        Create a symbolicator using the given symbol file name.
        '''
        self._filename = filename
        self._files = {}
        self._funcs = []
        self._initModule()

    def _initModule(self):
        '''
        Load MODULE record in symbol file.
        '''
        with open(self._filename, 'r') as f:
            line = f.readline().strip()
            assert line.startswith('MODULE ')

        label, plat, arch, breakpadId, name = line.split(' ', 4)
        self._architecture = arch
        self._breakpadId = breakpadId
        self._library = name
        self._platform = plat

    @property
    def architecture(self):
        '''
        The architecture targetted by the library.
        '''
        return self._architecture

    @property
    def breakpad_id(self):
        '''
        The breakpad ID of the library represented by the symbol file.
        '''
        return self._breakpadId

    @property
    def library(self):
        '''
        The name of the library represented by the symbol file.
        '''
        return self._library

    @property
    def platform(self):
        '''
        The platform targetted by the library.
        '''
        return self._platform

    def _listFiles(self):
        '''
        Load FILE records in symbol file.
        '''
        if self._files:
            return
        with open(self._filename, 'r') as f:
            line = f.readline()
            while line:
                if line.startswith('FILE '):
                    label, index, source = line.strip().split(' ', 2)
                    self._files[index] = source
                # FILE lines tend to be together at the beginning of the
                # symbol file, so if we found some FILE lines but stopped
                # seeting them, we can assume there are no more FILE lines.
                elif self._files:
                    break
                line = f.readline()

    def _getFile(self, index):
        self._listFiles()
        return self._files[index]

    def _listFuncs(self):
        '''
        Load FUNC records in symbol file.
        '''
        if self._funcs:
            return
        with open(self._filename, 'r') as f:
            line = f.readline()
            while line:
                if line.startswith('FUNC '):
                    label, start, size, stack, name = line.strip().split(' ', 4)
                    start = int(start, 0x10)
                    size = int(size, 0x10)
                    self._funcs.append(
                        self.Func(start, start + size, name, f.tell()))
                line = f.readline()

        self._funcs.sort(key=lambda f: f.start)
        self._funcKeys = [f.end for f in self._funcs]

    def _getFunc(self, address):
        self._listFuncs()
        index = bisect(self._funcKeys, address)
        if index == len(self._funcKeys) or address < self._funcs[0].start:
            return None
        return self._funcs[index]

    def symbolicate(self, address):
        '''
        Given an address relative to the library,
        return a Symbol corresponding to that address.
        '''
        func = self._getFunc(address)
        if not func:
            return None

        # Get filename and line number
        with open(self._filename, 'r') as f:
            f.seek(func.offset)
            line = f.readline()
            label = line.partition(' ')[0]
            while all(c in '0123456789abcdefABCDEF' for c in label):
                start, size, lineno, fileno = line.strip().split(' ', 3)
                start = int(start, 0x10)
                size = int(size, 0x10)
                if address >= start and address < (start + size):
                    return Symbol(self._library, func.name,
                                  self._getFile(fileno), int(lineno))
                line = f.readline()
                label = line.partition(' ')[0]

        return Symbol(self._library, func.name, '(unknown)', 0)

class Symbolicator:

    class Product(object):
        _SERVER = 'ftp.mozilla.org'
        _SCRATCH = '{appName}-{appBuildID}-{repo}-{platform}-{arch}'
        _SYM_EXT = '.sym'

        def __init__(self, params):
            self._params = params
            self._params['symarch'] = (
                'x86_64' if self._params['arch'] == 'x86-64' else
                'arm' if self._params['arch'].startswith('arm') else
                self._params['arch'])
            self._mods = {}

        def getScratch(self):
            return self._SCRATCH.format(**self._params)

        def getServer(self):
            return self._SERVER

        def getPath(self):
            return self._PATH.format(**self._params)

        def getFile(self):
            return self._FILE.format(**self._params)

        def getModules(self, scratch):
            for dirname in os.listdir(scratch):
                if os.path.isdir(os.path.join(scratch, dirname)):
                    yield os.path.join(scratch, dirname)

        def moduleMatches(self, stackMod, localMod):
            return stackMod[-1] == localMod[-1]

        def symbolicate(self, module, address):
            if module in self._mods:
                return self._mods[module].symbolicate(address)

            versions = os.listdir(module)
            while versions:
                sym = os.path.join(module, versions.pop())
                sym = os.path.join(sym, next(
                    f for f in os.listdir(sym) if f.endswith(self._SYM_EXT)))
                symfile = BreakpadSymbolFile(sym)
                if symfile.architecture == self._params['symarch']:
                    self._mods[module] = symfile
                    return symfile.symbolicate(address)

    class Desktop(Product):
        _PATH = ('/pub/mozilla.org/firefox/nightly/{build_Y}/{build_m}/'
            '{build_Y}-{build_m}-{build_d}-{build_H}-{build_M}-{build_S}-{repo}')
        _FILE = 'firefox-{appVersion}.en-US.{osArch}.crashreporter-symbols.zip'

        def __init__(self, params):
            params['osArch'] = (
                'linux-i686' if params['platform'] == 'Linux' and
                                params['arch'] == 'x86' else
                'linux-x86_64' if params['platform'] == 'Linux' and
                                  params['arch'] == 'x86-64' else
                'mac' if params['platform'] == 'Darwin' else
                'win32' if params['platform'] == 'WINNT' and
                           params['arch'] == 'x86' else
                'win64-x86_64' if params['platform'] == 'WINNT' and
                                  params['arch'] == 'x86-64' else
                'unknown'
            )
            super(self.__class__, self).__init__(params)

        def moduleMatches(self, stackMod, localMod):
            if self._params['platform'] != 'WINNT':
                return super(self.__class__, self).moduleMatches(stackMod, localMod)

            return (os.path.splitext(stackMod[-1])[0].lower() ==
                    os.path.splitext(localMod[-1])[0].lower())

        def splitPath(self, path):
            return path.split('/' if self._params['platform'] != 'WINNT' else
                              '\\')

    class Mobile(Product):
        _BASE_PATH = ('/pub/mozilla.org/mobile/nightly/{build_Y}/{build_m}/'
            '{build_Y}-{build_m}-{build_d}-{build_H}-{build_M}-{build_S}-{repo}-android')
        _ARCH_PATH = '-{arch}'
        _SUFFIX_PATH = '/en-US'
        _FILE = 'fennec-{appVersion}.en-US.android-{abi}.crashreporter-symbols.zip'
        _ABI = {'armv7': 'arm', 'armv6': 'arm-armv6', 'x86': 'i386'}

        def __init__(self, params):
            params['abi'] = self._ABI[params['arch']]
            super(self.__class__, self).__init__(params)

        def getPath(self):
            path = self._BASE_PATH.format(**self._params)
            if self._params['arch'] != 'armv7':
                path += self._ARCH_PATH.format(**self._params)
            path += self._SUFFIX_PATH
            return path

        def splitPath(self, path):
            return path.split('/')

    @classmethod
    def fromBuild(cls, scratch, info):
        info = dict(info)
        product = None
        if info.get('appName', '') == 'Fennec':
            product = cls.Mobile
        elif info.get('appName', '') == 'Firefox':
            product = cls.Desktop
        else:
            return None
        if 'appBuildID' in info:
            build = info['appBuildID']
            if '-' in build:
                build = build.partition('-')[-1]
            buildDate = datetime.strptime(build, '%Y%m%d%H%M%S')
            for val in 'YmdHMS':
                info['build_' + val] = buildDate.strftime('%' + val)
        if 'appUpdateChannel' in info:
            channel = info['appUpdateChannel']
            if channel == 'nightly':
                info['repo'] = 'mozilla-central'
            elif channel == 'aurora':
                info['repo'] = 'mozilla-aurora'
            elif channel.startswith('nightly-'):
                info['repo'] = channel.partition('-')[-1]
            else:
                return None
        return cls(scratch, product(info))

    def __init__(self, scratch, product):
        self._product = product
        self._scratch = os.path.join(scratch, product.getScratch())

    def symbolicate(self, module, address):
        device_mod = self._product.splitPath(module)
        local_mods = list(self._product.getModules(self._scratch))

        matching_mods = [mod for mod in local_mods if
            self._product.moduleMatches(device_mod, mod.split(os.path.sep))]

        if len(matching_mods) != 1:
            raise Exception("Cannot find module")

        return self._product.symbolicate(matching_mods[0], address)

    def extractSymbols(self, archive, scratch):
        with zipfile.ZipFile(archive, 'r') as arc:
            namelist = arc.namelist()
            if all(os.path.exists(os.path.join(scratch, n)) for n in namelist):
                return
            if not all(os.path.realpath(os.path.join(scratch, f)).startswith(
                       os.path.realpath(scratch)) for f in namelist):
                raise Exception("Invalid archive")
            arc.extractall(scratch)

    def fetchSymbols(self):
        if not os.path.exists(self._scratch):
            os.makedirs(self._scratch)
        path = self._product.getPath()
        fname = self._product.getFile()
        dst = os.path.join(self._scratch, fname)

        def download(ftp, src, dst):
            with open(dst, 'wb') as f:
                def write(s):
                    f.write(s)
                ftp.retrbinary('RETR ' + src, write)

        if not os.path.exists(dst):
            ftp = ftplib.FTP(self._product.getServer())
            try:
                ftp.login()
                download(ftp, path + '/' + fname, dst)
            finally:
                ftp.quit()

        if not os.path.exists(dst):
            raise Exception("Cannot download symbols")

        self.extractSymbols(dst, self._scratch)

def symbolicateStack(stack, sym=None, scratch=None, info=None):
    if not sym:
        sym = Symbolicator.fromBuild(scratch, info)
        try:
            sym.fetchSymbols()
        except:
            for frame in (str(f) for f in stack):
                yield frame
            return

    for frame in (str(f) for f in stack):
        if not frame.startswith('c:'):
            # only C stacks need symbolicating
            yield frame
            continue
        ident, lib, addr = frame.split(':', 2)
        if not addr or not addr[0].isdigit():
            yield frame
            continue
        try:
            symbol = sym.symbolicate(lib, int(addr, 0))
            func = symbol.function
            if symbol.source or symbol.line:
                func += ' (%s:%s)' % (symbol.source, str(symbol.line))
            yield ':'.join((ident, symbol.library, func))
        except:
            yield frame
