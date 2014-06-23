import re
import simplejson as json
from collections import OrderedDict
from anr import ANRReport
import mapreduce_common

mapreduce_common.allowed_infos = mapreduce_common.allowed_infos_anr
mapreduce_common.allowed_dimensions = mapreduce_common.allowed_dimensions_anr

re_subname = re.compile(r'\$\w*\d+')

def processFrame(frame):
    return re_subname.sub('$', frame)

def map(slug, dims, value, context):
    anr = ANRReport(value)
    mainThread = anr.mainThread
    if not mainThread:
        return
    stack = mainThread.stack
    stack = [str(frame).split(':')[1] for frame in stack
             if not frame.isNative]

    def filterStack(stack):
        # least stable to most stable
        ignoreList = [
            'com.android.internal.',
            'com.android.',
            'dalvik.',
            'android.',
            'java.lang.',
        ]
        def getStack(s):
            return list(OrderedDict.fromkeys(
                [processFrame(frame) for frame in s
                 if not any(frame.startswith(prefix) for prefix in ignoreList)]))
        out = getStack(stack)
        while ignoreList and len(out) < 10:
            ignoreList.pop()
            out = getStack(stack)
        return out

    key_thread = mainThread.name
    key_stack = filterStack(stack)

    def getNativeStack():
        nativeThread = 'Gecko (native)'
        nativeMain = anr.getThread(nativeThread)
        if nativeMain is None:
            nativeMain = anr.getThread('GeckoMain (native)')
        if nativeMain is None:
            nativeThread = 'Gecko'
            nativeMain = anr.getThread(nativeThread)
        if nativeMain is None:
            return (key_thread, key_stack)
        nativeStack = filterStack([str(f).partition('+')[0]
            for f in nativeMain.stack if f.isPseudo or not f.isNative])
        if not nativeStack:
            return (key_thread, key_stack)
        return (nativeThread, nativeStack)
    if any('sendEventToGeckoSync' in f for f in key_stack):
        key_thread, key_stack = getNativeStack()

    context.write((key_thread, tuple(key_stack)), (
        dims + [slug],
        mapreduce_common.filterDimensions(
            dims, mapreduce_common.filterInfo(anr.rawData['info'])),
        value))

def reduce(key, values, context):
    if not values or len(values) < 5:
        return
    info = {}
    anrs = []
    slugs = []
    for slug, dims, value in values:
        anr = ANRReport(value)
        anrs.append(anr)
        raw_info = mapreduce_common.filterInfo(anr.rawData['info'])
        mapreduce_common.addUptime(raw_info, anr.rawData)
        for dimname, dim in dims.iteritems():
            diminfo = info.setdefault(dimname, {}).setdefault(dim, {})
            for infokey, infovalue in raw_info.iteritems():
                counts = diminfo.setdefault(infokey, {})
                counts[infovalue] = counts.get(infovalue, 0) + 1
        slugs.append(slug)

    key_thread = key[0]
    def filterThreadName(name):
        if name == 'GeckoMain (native)':
            return 'Gecko (native)'
        return name
    def anrCmp(left, right):
        def findThread(anr):
            main = anr.mainThread
            if main and main.name == key_thread:
                return main
            for thread in anr.getBackgroundThreads():
                if filterThreadName(thread.name) == key_thread:
                    return thread
            return None
        left_thread = findThread(left)
        right_thread = findThread(right)
        if (left_thread and right_thread and
            len(left_thread.stack) != len(right_thread.stack)):
            return cmp(len(left_thread.stack), len(right_thread.stack))
        return cmp(left.detail, right.detail)
    sample = anrs[0]
    for anr in anrs:
        if anr is sample:
            continue
        if anrCmp(sample, anr) < 0:
            sample = anr

    context.write(slugs[0], json.dumps({
        'info': info,
        'threads': [{
                'name': sample.mainThread.name,
                'stack': [str(f) for f in sample.mainThread.stack]
            }] + [{
                'name': filterThreadName(t.name),
                'stack': [str(f) for f in t.stack]
            } for t in sample.getBackgroundThreads()],
        'slugs': slugs,
        'display': key_thread,
        'symbolicatorInfo': sample.rawData['info'],
    }, separators=(',', ':')))
