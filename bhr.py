# Same as the osdistribution.py example in jydoop
import json
import mapreduce_common

def map(raw_key, raw_dims, raw_value, cx):
    if 'threadHangStats' not in raw_value:
        return
    try:
        j = json.loads(raw_value)
        raw_info = j['info']
        info = mapreduce_common.filterInfo(raw_info)
        mapreduce_common.addUptime(info, j)
        dims = mapreduce_common.filterDimensions(raw_dims, info)
        uptime = j['simpleMeasurements']['uptime']
        for thread in j['threadHangStats']:
            name = thread['name']
            cx.write((name, None), (dims, info, thread['activity']))
            for hang in thread['hangs']:
                cx.write((name, tuple(hang['stack'])),
                         (dims, info, hang['histogram']))
        cx.write((None,), (dims, info, uptime))
    except KeyError:
        pass

def reduce(raw_key, raw_values, cx):
    result = {}

    upper = lower = None
    if raw_key[0] is None:
        upper = mapreduce_common.quantile(raw_values, 10, upper=True, key=lambda x:x[2])
        lower = mapreduce_common.quantile(raw_values, 10, upper=False, key=lambda x:x[2])

    def merge(dest, src):
        # dest and src are dicts of buckets and counts
        for k, v in src.iteritems():
            dest[k] = dest.get(k, 0) + v

    def collect(dim, info, counts):
        if not isinstance(counts, dict):
            # int
            if ((upper is not None and counts > upper) or
                (lower is not None and counts < lower)):
                return
            for k, v in info.iteritems():
                info_bucket = dim.setdefault(k, {})
                info_bucket[v] = info_bucket.get(v, 0) + counts
            return
        for k, v in info.iteritems():
            info_bucket = dim.setdefault(k, {})
            if v not in info_bucket:
                info_bucket[v] = counts['values']
                continue
            merge(info_bucket[v], counts['values'])

    # uptime measurement
    for dims, info, counts in raw_values:
        for k, dim_val in dims.iteritems():
            collect(result.setdefault(k, {}).setdefault(dim_val, {}),
                    info, counts)

    cx.write(json.dumps(raw_key), json.dumps(result))