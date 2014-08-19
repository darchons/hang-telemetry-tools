import __builtin__
import itertools
import mapreduce_common
import math
import re
import simplejson as json
import datetime
import uuid

mapreduce_common.allowed_infos = mapreduce_common.allowed_infos_bhr
mapreduce_common.allowed_dimensions = mapreduce_common.allowed_dimensions_bhr

FILTER_PASS = 0
DATA_PASS = 1

if __name__ == 'mapreduce-bhr-filter':
    PASS = FILTER_PASS
elif __name__ == 'mapreduce-bhr':
    PASS = DATA_PASS
else:
    raise Exception('Unknown pass')

SKIP = 0
FILTER_LIMIT = 10

RE_LINE = re.compile(r':\d+$')
RE_ADDR = re.compile(r':0x[\da-f]+$', re.IGNORECASE)
RE_THREAD_NAME_NUM = re.compile(r'[^a-zA-Z]*\d+$')

ARCH_PRIO = 'armv7 x86-64 x86'
# Treat Darwin, Linux, Android as having same priority,
# because they have similar telemetry submission rates.
PLAT_PRIO = 'WINNT'
CHAN_PRIO = 'release beta aurora nightly'

SUMMARY = {}
with open('summary.txt', 'r') as f:
    for line in f:
        info, sep, stats = line.partition('\t')
        info = json.loads(info)
        stats = json.loads(stats)
        SUMMARY.setdefault(info[0], {})[info[1]] = stats[-1]

if PASS != FILTER_PASS:
    FILTER = {}
    with open('filter.txt', 'r') as f:
        for line in f:
            info, sep, stats = line.partition('\t')
            info = json.loads(info)
            stats = json.loads(stats)
            FILTER.setdefault(info[0], {}).setdefault(
                info[1], []).append(tuple(stats[-1]))

# Cut off reports from before 12 weeks (two releases) ago.
BUILDID_CUTOFF = (
    datetime.date.today() - datetime.timedelta(weeks=12)
).strftime('%Y%m%d%H%M%S')

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

        if raw_info.get('appBuildID') < BUILDID_CUTOFF:
            return

        info = mapreduce_common.filterInfo(raw_info)
        mapreduce_common.addUptime(info, j)
        dims = mapreduce_common.filterDimensions(raw_dims, info)
    except KeyError:
        return

    def filterFrame(frame):
        frame = RE_LINE.sub('', frame)
        return frame

    FRAME_BLACKLIST = [
        'js::RunScript',
    ]

    def filterStack(stack):
        return (filterFrame(x[0]) for x in itertools.groupby(stack)
                                  if x[0] not in FRAME_BLACKLIST)

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
            if (uptime >= SUMMARY[dim_key][dim_val][0] and
                uptime <= SUMMARY[dim_key][dim_val][-1])
        })
    collectedUptime = collectData(dims, info, uptime)

    def formatStack(stack):
        for frame in reversed(stack):
            if RE_ADDR.search(frame):
                yield 'c:' + frame
                continue
            if ':' not in frame:
                yield 'p:' + frame
                continue
            if 'revision' not in raw_info:
                yield 'p:' + frame
                continue
            parts = raw_info['revision'].split('/')
            yield 'p:' + frame + ' (mxr:' + parts[-3] + ':' + parts[-1] + ')'

    def collectStack(dims, info, name, hang):
        return (
            (
                tuple(formatStack(hang['stack'])),
                (
                    info['appUpdateChannel'],
                    info['appVersion'],
                    info['appBuildID']
                )
            ),
            {
                dim_key: {
                    dim_val: (
                        tuple(formatStack(hang['nativeStack'])),
                        info
                    )
                }
                for dim_key, dim_val in dims.iteritems()
            }
            if 'nativeStack' in hang else None
        )

    def filterThreadName(name):
        name = RE_THREAD_NAME_NUM.sub('', name)
        return name

    if PASS == FILTER_PASS:
        for thread in j['threadHangStats']:
            name = filterThreadName(thread['name'])
            for hang in thread['hangs']:
                if not hang['stack']:
                    continue
                for dim_key, dim_val in dims.iteritems():
                    if (uptime < SUMMARY[dim_key][dim_val][0] or
                        uptime > SUMMARY[dim_key][dim_val][-1]):
                        continue
                    cx.write((dim_key, dim_val),
                             (1, (name, tuple(filterStack(hang['stack'])))))
        return

    assert PASS == DATA_PASS

    for thread in j['threadHangStats']:
        name = filterThreadName(thread['name'])
        cx.write((name, None),
                 collectData(dims, info, thread['activity']['values']))
        for hang in thread['hangs']:
            if not hang['stack']:
                continue

            stack = tuple(filterStack(hang['stack']))

            if not any(stack in FILTER[dim_key][dim_val]
                       for dim_key, dim_val in dims.iteritems()):
                continue

            cx.write((name, stack),
                     collectData(dims, info, hang['histogram']['values']) +
                     (collectStack(dims, info, name, hang),))

        cx.write((None, name), collectedUptime)

    if j['threadHangStats']:
        cx.write((None, None), collectedUptime)

def filter_reduce(raw_key, raw_values):
    if not raw_values:
        return

    def _get_groups():
        sum_count = 0
        sum_stack = raw_values[0]
        raw_values.sort(key=lambda x: x[1])

        for count, stack in raw_values:
            if stack != sum_stack:
                yield (sum_count, sum_stack)
                sum_count = 0
                sum_stack = stack
            sum_count += count
        yield (sum_count, sum_stack)

    for group in sorted(_get_groups(),
                        key=lambda x: x[0],
                        reverse=True)[:FILTER_LIMIT]:
        cx.write(raw_key, group)

def data_do_combine(raw_key, raw_values):
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

            # appUpdateChannel
            prio = (CHAN_PRIO.find(leftStack[1][0]) -
                    CHAN_PRIO.find(rightStack[1][0]))
            if prio != 0:
                return left if prio > 0 else right

            # appVersion
            prio = cmp(mapreduce_common.partitionVersion(leftStack[1][1]),
                       mapreduce_common.partitionVersion(rightStack[1][1]))
            if prio != 0:
                return left if prio > 0 else right

            # appBuildID
            return left if leftStack[1][2] >= rightStack[1][2] else right

        if not leftNative or not rightNative:
            # one has native stack
            return left if leftNative else right

        leftPseudo = leftStack[0]
        rightPseudo = rightStack[0]

        def starts_with_pseudo(native, pseudo):
            return native[0: len(pseudo)] == pseudo

        def merge_native_stack_info(left, right):
            leftNative, leftInfo = left
            rightNative, rightInfo = right

            leftStartsWithPseudo = starts_with_pseudo(leftNative, leftPseudo)
            rightStartsWithPseudo = starts_with_pseudo(rightNative, rightPseudo)

            if leftStartsWithPseudo != rightStartsWithPseudo:
                # because the native stack is taken some time after the pseudostack,
                # the native stack may not correspond to the pseudostack anymore.
                # so we prefer the native stack that starts with the pseudostack.
                return left if leftStartsWithPseudo else right

            prio = (ARCH_PRIO.find(leftInfo['arch']) -
                    ARCH_PRIO.find(rightInfo['arch']))
            if prio != 0:
                return left if prio > 0 else right

            prio = (PLAT_PRIO.find(leftInfo['platform']) -
                    PLAT_PRIO.find(rightInfo['platform']))
            if prio != 0:
                return left if prio > 0 else right

            prio = cmp(mapreduce_common.partitionVersion(leftInfo['appVersion']),
                       mapreduce_common.partitionVersion(rightInfo['appVersion']))
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

def data_combine(raw_key, raw_values, cx):
    key, value = data_do_combine(raw_key, raw_values)
    cx.write(key, value)

def data_reduce(raw_key, raw_values, cx):
    if (not raw_values or
        sum(x[0] for x in raw_values) < 10):
        return

    key, value = data_do_combine(raw_key, raw_values)

    def sumLogHistogram(info_vals, quantiles):
        return {info_val: sum(invlog(log) * count
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
        key = (key[0], str(uuid.uuid4()))

    cx.write(json.dumps(key, separators=(',', ':')),
             json.dumps(value[1:], separators=(',', ':')))

if PASS == FILTER_PASS:
    reduce = filter_reduce

elif PASS == DATA_PASS:
    combine = data_combine
    reduce = data_reduce
