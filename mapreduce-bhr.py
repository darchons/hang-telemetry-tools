import __builtin__
import itertools
import mapreduce_common
import math
import re
import simplejson as json
import uuid

mapreduce_common.allowed_infos = mapreduce_common.allowed_infos_bhr
mapreduce_common.allowed_dimensions = mapreduce_common.allowed_dimensions_bhr

SKIP = 49

RE_LINE = re.compile(r':\d+$')
RE_ADDR = re.compile(r':0x[\da-f]+$', re.IGNORECASE)

ARCH_PRIO = 'armv7 x86-64 x86'
# Treat Darwin, Linux, Android as having same priority,
# because they have similar telemetry submission rates.
PLAT_PRIO = 'WINNT'

def log(x):
    return round(math.log(x + 1), 2)

def invlog(x):
    return int(round(math.exp(x) - 1))

def map(raw_key, raw_dims, raw_value, cx):
    if SKIP > 0 and (hash(raw_key) % (SKIP + 1)) != 0:
        return
    if '"threadHangStats":' not in raw_value:
        return
    try:
        j = json.loads(raw_value)
        raw_sm = j['simpleMeasurements']
        uptime = raw_sm['uptime']
        if uptime < 0:
            return
        if raw_sm.get('debuggerAttached', 0):
            return
        raw_info = j['info']
        info = mapreduce_common.filterInfo(raw_info)
        mapreduce_common.addUptime(info, j)
        dims = mapreduce_common.filterDimensions(raw_dims, info)
    except KeyError:
        return

    def filterFrame(frame):
        frame = RE_LINE.sub('', frame)
        return frame

    def filterStack(stack):
        return (filterFrame(x[0]) for x in itertools.groupby(stack))

    def collectData(dims, info, data):
        if isinstance(data, dict):
            data = {k: v * (SKIP + 1) for k, v in data.iteritems()
                    if v and k.isdigit()}
        else:
            data = {log(data): SKIP + 1}
        return (1, {
            dim_key: {
                dim_val: {
                    info_key: {
                        info_val: data
                    }
                    for info_key, info_val in info.iteritems()
                }
            }
            for dim_key, dim_val in dims.iteritems()
        })
    collectedUptime = collectData(dims, info, uptime)

    def formatStack(stack):
        for frame in reversed(stack):
            if RE_ADDR.search(frame):
                yield 'c:' + frame
            if not RE_LINE.search(frame):
                yield 'p:' + frame
            if 'revision' not in raw_info:
                yield 'p:' + frame
            parts = raw_info['revision'].split('/')
            yield 'p:' + frame + ' (mxr:' + parts[-3] + ':' + parts[-1] + ')'

    def collectStack(dims, info, name, hang):
        return (
            (
                formatStack(hang['stack']),
                info['appBuildID']
            },
            {
                dim_key: {
                    dim_val: (
                        formatStack(hang['nativeStack']),
                        info
                    )
                }
                for dim_key, dim_val in dims.iteritems()
            }
            if 'nativeStack' in hang else None
        )

    for thread in j['threadHangStats']:
        name = thread['name']
        cx.write((name, None),
                 collectData(dims, info, thread['activity']['values']))
        for hang in thread['hangs']:
            if not hang['stack']:
                continue
            cx.write((name, tuple(filterStack(hang['stack']))),
                     collectData(dims, info, hang['histogram']['values']) +
                     (collectStack(dims, info, name, hang),))
        cx.write((None, name), collectedUptime)
    if j['threadHangStats']:
        cx.write((None, None), collectedUptime)

def do_combine(raw_key, raw_values):
    def merge_dict(left, right):
        for k, v in right.iteritems():
            if not isinstance(v, dict):
                left[k] = left.get(k, 0) + v
                continue
            if k not in left:
                left[k] = v
                continue
            merge_dict(left[k], v)
        return left

    def merge_stack(left, right):
        leftStack, leftNative = left
        rightStack, rightNative = right

        if not leftNative and not rightNative:
            # neither has native stack
            return left if leftStack[1] >= rightStack[1] else right

        if not leftNative or not rightNative:
            # one has native stack
            return left if leftNative else right

        def merge_native_stack_info(left, right):
            leftInfo = left[1]
            rightInfo = right[1]

            prio = (ARCH_PRIO.find(leftInfo['arch']) -
                    ARCH_PRIO.find(rightInfo['arch']))
            if prio != 0:
                return left if prio > 0 else right

            prio = (PLAT_PRIO.find(leftInfo['platform']) -
                    PLAT_PRIO.find(rightInfo['platform']))
            if prio != 0:
                return left if prio > 0 else right

            prio = cmp(leftInfo['appVersion'],
                       rightInfo['appVersion'])
            if prio != 0:
                return left if prio > 0 else right

            if leftInfo['appBuildID'] >= rightInfo['appBuildID']:
                return left
            return right

        # both have native stacks
        for dim_key, dim_vals in leftNative.iteritems():
            if dim_key not in rightNative:
                continue
            for dim_val, native_stack_info in rightNative[dim_key].iteritems():
                if dim_val not in dim_vals:
                    dim_vals[dim_val] = native_stack_info
                    continue
                dim_vals[dim_val] = merge_native_stack_info(
                    dim_vals[dim_val], native_stack_info)

        return left

    def merge(left, right):
        if len(left) < 3 or len(right) < 3:
            return (left[0] + right[0], merge_dict(left[1], right[1]))

        return (left[0] + right[0],
                merge_dict(left[1], right[1]),
                merge_stack(left[2], right[2]))

    return raw_key, __builtin__.reduce(merge, raw_values)

def combine(raw_key, raw_values, cx):
    key, value = do_combine(raw_key, raw_values)
    cx.combine_size = 30
    cx.write(key, value)

def reduce(raw_key, raw_values, cx):
    if (not raw_values or
        sum(x[0] for x in raw_values) < 10):
        return

    key, value = do_combine(raw_key, raw_values)

    def sumLogHistogram(info_vals, quantiles):
        keys = sorted((log, count)
                      for histogram in info_vals.itervalues()
                      for log, count in histogram.iteritems())
        limit = sum(count for log, count in keys) / quantiles
        def findBound(keys):
            remaining = limit
            for i, (log, count) in enumerate(keys):
                remaining -= count
                if remaining < 0:
                    return log
        lower = findBound(keys)
        keys.reverse()
        upper = findBound(keys)
        return {info_val: sum(invlog(min(max(log, lower), upper)) * count
                              for log, count in histogram.iteritems())
                for info_val, histogram in info_vals.iteritems()}

    def sumUptimes(histograms, quantiles):
        for dim_name, dim_vals in histograms.iteritems():
            for dim_val, info_names in dim_vals.iteritems():
                for info_name, info_vals in info_names.iteritems():
                    info_names[info_name] = sumLogHistogram(info_vals, quantiles)

    if key[0] is None:
        sumUptimes(value[1], 10)
    elif key[1] is not None:
        key[1] = str(uuid.uuid4())

    cx.write(json.dumps(key, separators=(',', ':')),
             json.dumps(value[1:], separators=(',', ':')))

