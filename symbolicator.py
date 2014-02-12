#!/usr/bin/env python2

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from datetime import datetime
import ftplib
import os
import subprocess
import zipfile

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
