import __builtin__
import itertools
import mapreduce_common
import math
import simplejson as json

mapreduce_common.allowed_infos = mapreduce_common.allowed_infos_bhr
mapreduce_common.allowed_dimensions = mapreduce_common.allowed_dimensions_bhr

SKIP = 0

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

    def filterStack(stack):
        return (x[0] for x in itertools.groupby(stack))

    def collectData(dims, info, data):
        if isinstance(data, dict):
            data = {k: v for k, v in data.iteritems()
                    if v and k.isdigit()}
        else:
            data = {log(data): 1}
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

    for thread in j['threadHangStats']:
        name = thread['name']
        cx.write((name, None),
                 collectData(dims, info, thread['activity']['values']))
        for hang in thread['hangs']:
            if not hang['stack']:
                continue
            cx.write((name, tuple(filterStack(hang['stack']))),
                     collectData(dims, info, hang['histogram']['values']))
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
    def merge(left, right):
        return (left[0] + right[0], merge_dict(left[1], right[1]))
    return raw_key, __builtin__.reduce(merge, raw_values)

def combine(raw_key, raw_values, cx):
    key, value = do_combine(raw_key, raw_values)
    cx.combine_size = 200
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

    if raw_key[0] is None:
        sumUptimes(value[1], 10)

    cx.write(json.dumps(key, separators=(',', ':')),
             json.dumps(value[1], separators=(',', ':')))

