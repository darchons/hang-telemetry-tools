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

    class Mobile:
        _SERVER = 'ftp.mozilla.org'
        _BASE_PATH = ('/pub/mozilla.org/mobile/nightly/{build_Y}/{build_m}/'
            '{build_Y}-{build_m}-{build_d}-{build_H}-{build_M}-{build_S}-{repo}-android')
        _ARCH_PATH = '-{arch}'
        _SUFFIX_PATH = '/en-US'
        _FILE = 'fennec-{appVersion}.en-US.android-{abi}.apk'
        _SCRATCH = '{appBuildID}-{repo}-{arch}'
        _ABI = {'armv7': 'arm', 'armv6': 'arm-armv6', 'x86': 'i386'}
        _MOD_EXT = '.so'

        def __init__(self, params):
            params['abi'] = self._ABI[params['arch']]
            self._params = params

        def getScratch(self):
            return self._SCRATCH.format(**self._params)

        def getServer(self):
            return self._SERVER

        def getPath(self):
            path = self._BASE_PATH.format(**self._params)
            if self._params['arch'] != 'armv7':
                path += self._ARCH_PATH.format(arch=self._params['arch'])
            path += self._SUFFIX_PATH
            return path

        def getFile(self):
            return self._FILE.format(**self._params)

        def extractBinaries(self, archive, scratch):
            arc = zipfile.ZipFile(archive, 'r')
            try:
                namelist = arc.namelist()
                if all(os.path.exists(os.path.join(scratch, n)) for n in namelist):
                    return
                if not all(os.path.realpath(os.path.join(scratch, f)).startswith(
                           os.path.realpath(scratch)) for f in namelist):
                    raise Exception("Invalid archive")
                arc.extractall(scratch)
                for soname in (n for n in namelist if n.endswith(self._MOD_EXT)):
                    subprocess.call([
                        os.path.abspath(os.path.join(
                            os.path.dirname(__file__), 'szip')),
                        '-d', os.path.join(scratch, soname)])
            finally:
                arc.close()

        def splitPath(self, path):
            return path.split('/')

        def getModules(self, scratch):
            for dirpath, dirnames, filenames in os.walk(scratch):
                for fname in filenames:
                    if fname.endswith(self._MOD_EXT):
                        yield os.path.join(dirpath, fname)

        # returns (library, function, file, line)
        def symbolicate(self, module, address):
            func, file_line = subprocess.check_output([
                os.path.abspath(os.path.join(
                    os.path.dirname(__file__), 'arm-addr2line')),
                '-e', module, '-ifsC', address]).splitlines()
            fn, delim, ln = file_line.partition(':')
            return (os.path.basename(module),
                func if func.strip('?') else '',
                fn if fn.strip('?') else '',
                ln if ln.strip('?').strip('0') else '')

    @classmethod
    def fromBuild(cls, scratch, info):
        info = dict(info)
        product = None
        if info.get('appName', '') == 'Fennec':
            product = cls.Mobile
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
        return cls(scratch, product(info))

    def __init__(self, scratch, product):
        self._product = product
        self._scratch = os.path.join(scratch, product.getScratch())

    # returns (library, function, file, line)
    def symbolicate(self, module, address):
        device_mod = self._product.splitPath(module)
        local_mods = list(self._product.getModules(self._scratch))
        depth = 0

        while not depth or len(matching_mods) > 1:
            depth += 1
            matching_mods = []
            for mod in local_mods:
                local_mod = mod.split(os.path.sep)
                if device_mod[-depth:] == local_mod[-depth:]:
                    matching_mods.append(mod)
        if not len(matching_mods):
            raise Exception("Cannot find module")
        return self._product.symbolicate(matching_mods[0], address)

    def fetchBinaries(self):
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
            raise Exception("Cannot download binaries")

        self._product.extractBinaries(dst, self._scratch)

def symbolicateStack(stack, sym=None, scratch=None, info=None):
    if not sym:
        sym = Symbolicator.fromBuild(scratch, info)
        sym.fetchBinaries()
    for frame in (str(f) for f in stack):
        if not frame.startswith('c:'):
            # only C stacks need symbolicating
            yield frame
            continue
        ident, lib, addr = frame.split(':', 2)
        if not addr[0].isdigit():
            yield frame
            continue
        try:
            lib, func, fn, ln = sym.symbolicate(lib, addr)
            if fn or ln:
                func += ' (%s:%s)' % (fn, str(ln))
            yield ':'.join((ident, lib, func))
        except:
            yield frame
