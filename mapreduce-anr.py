import re
import simplejson as json
from collections import OrderedDict
from anr import ANRReport
import mapreduce_common

mapreduce_common.allowed_infos = mapreduce_common.allowed_infos_anr
mapreduce_common.allowed_dimensions = mapreduce_common.allowed_dimensions_anr

re_subname = re.compile(r'\$\w*\d+')

ARCH_PRIO = 'armv7 x86'
CHAN_PRIO = 'aurora nightly'

def processFrame(frame):
    return re_subname.sub('$', frame)

def map(slug, dims, value, context):
    anr = ANRReport(value)
    mainThread = anr.mainThread
    if not mainThread:
        return

    def getFrameKey(frame):
        if frame.isPseudo:
            return str(frame).partition('+')[0]
        if not frame.isNative:
            return str(frame).split(':')[1]
        return str(frame).partition(':')[-1]

    stack = mainThread.stack
    stack = [getFrameKey(frame) for frame in stack
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
        nativeStack = filterStack([getFrameKey(f)
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
    out_info = {}
    anrs = []
    slugs = []
    for slug, dims, value in values:
        anr = ANRReport(value)
        full_info = dict(anr.rawData['info'])
        mapreduce_common.adjustInfo(full_info)
        raw_info = mapreduce_common.filterInfo(anr.rawData['info'])
        mapreduce_common.addUptime(raw_info, anr.rawData)
        anrs.append((dims, full_info, anr))
        for dimname, dim in dims.iteritems():
            diminfo = out_info.setdefault(dimname, {}).setdefault(dim, {})
            for infokey, infovalue in raw_info.iteritems():
                counts = diminfo.setdefault(infokey, {})
                counts[infovalue] = counts.get(infovalue, 0) + 1
        slugs.append(slug)

    key_thread = key[0]

    def filterThreadName(name):
        return name.replace('GeckoMain', 'Gecko')

    def findKeyThread(anr):
        main = anr.mainThread
        if main and main.name == key_thread:
            return main
        for thread in anr.getBackgroundThreads():
            if filterThreadName(thread.name) == key_thread:
                return thread
        return None

    def merge_anr(left, right):
        if not left or not right:
            return left if left else right

        left_dims, left_info, left_anr = left
        right_dims, right_info, right_anr = right

        left_native = any('(native)' in thr.name
                          for thr in left_anr.getBackgroundThreads())
        right_native = any('(native)' in thr.name
                           for thr in right_anr.getBackgroundThreads())

        if not left_native and not right_native:
            prio = (CHAN_PRIO.find(left_info['appUpdateChannel']) -
                    CHAN_PRIO.find(right_info['appUpdateChannel']))
            if prio != 0:
                return left if prio > 0 else right

            prio = cmp(mapreduce_common.partitionVersion(left_info['appVersion']),
                       mapreduce_common.partitionVersion(right_info['appVersion']))
            if prio != 0:
                return left if prio > 0 else right

            return (left if left_info['appBuildID'].split('-')[-1] >=
                            right_info['appBuildID'].split('-')[-1] else right)

        if not left_native or not right_native:
            return left if left_native else right

        prio = (ARCH_PRIO.find(left_info['arch']) -
                ARCH_PRIO.find(right_info['arch']))
        if prio != 0:
            return left if prio > 0 else right

        prio = cmp(mapreduce_common.partitionVersion(left_info['appVersion']),
                   mapreduce_common.partitionVersion(right_info['appVersion']))
        if prio != 0:
            return left if prio > 0 else right

        prio = cmp(left_info['appBuildID'].split('-')[-1],
                   right_info['appBuildID'].split('-')[-1])
        if prio != 0:
            return left if prio > 0 else right

        left_thread = findKeyThread(left_anr)
        right_thread = findKeyThread(right_anr)
        if left_thread and right_thread:
            prio = cmp(len(left_thread.stack), len(right_thread.stack))
            if prio != 0:
                return left if prio > 0 else right

        prio = cmp(left_anr.detail, right_anr.detail)
        return left if prio >= 0 else right

    dim_threads = {}
    for tup in anrs:
        dims, info, anr = tup
        for dimname, dimval in dims.iteritems():
            threads = dim_threads.setdefault(dimname, {})
            threads[dimval] = merge_anr(threads.get(dimval), tup)

    display_thread = key_thread + ' key'
    out_threads = [{
        'name': display_thread,
        'stack': key[-1],
        'info': None
    }]

    for dimname, threads in dim_threads.iteritems():
        for dimval, tup in threads.iteritems():
            dims, info, anr = tup
            main = anr.mainThread
            out_threads.append({
                'name': '%s (dim:%s:%s)' % (main.name, dimname, dimval),
                'stack': [str(f) for f in main.stack],
                'info': info
            })
            out_threads.extend({
                'name': '%s (dim:%s:%s)' % (
                    filterThreadName(thr.name), dimname, dimval),
                'stack': [str(f) for f in thr.stack],
                'info': info
            } for thr in anr.getBackgroundThreads())

    context.write(slugs[0], json.dumps({
        'info': out_info,
        'threads': out_threads,
        'slugs': slugs,
        'display': display_thread
    }, separators=(',', ':')))
