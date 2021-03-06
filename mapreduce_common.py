import math

allowed_infos = None
allowed_dimensions = None

allowed_infos_anr = [
    'appUpdateChannel',
    'appVersion',
    'appBuildID',
    'locale',
    'device',
    'cpucount',
    'memsize',
    'os',
    'arch',
    'uptime',
]

allowed_dimensions_anr = [
    'appName',
    'appVersion',
    'arch',
    'cpucount',
    'memsize',
    'os',
    'submission_date',
]

allowed_infos_bhr = [
    'appName',
    'appUpdateChannel',
    'appVersion',
    'appBuildID',
    'locale',
    'cpucount',
    'memsize',
    'os',
    'arch',
    'platform',
    'adapterVendorID',
    'uptime',
]

allowed_dimensions_bhr = [
    'appName',
    'appVersion',
    'arch',
    'cpucount',
    'memsize',
    'platform',
    'submission_date',
]

dimensions = [
    'reason',
    'appName',
    'appUpdateChannel',
    'appVersion',
    'appBuildID',
    'submission_date',
]

def addUptime(info, ping):
    uptime = ping['simpleMeasurements']['uptime']
    if uptime >= 40320:
        uptime = '>4w'
    elif uptime >= 10080:
        uptime = '1w-4w'
    elif uptime >= 1440:
        uptime = '1d-1w'
    elif uptime >= 240:
        uptime = '3h-1d'
    elif uptime >= 30:
        uptime = '30m-3h'
    elif uptime >= 5:
        uptime = '5m-30m'
    elif uptime >= 1:
        uptime = '1m-5m'
    elif uptime >= 0:
        uptime = '<1m'
    else:
        return
    info['uptime'] = uptime

MEMSIZES = [(int((1 << n) * (mult + 0.25)), int((1 << n) * mult))
    for n in range(7, 30) for mult in (1, 1.5)]

def roundMemSize(n):
    out = next(size for bound, size in MEMSIZES if bound >= n)
    if out < 1024:
        return str(out) + 'M'
    if out > 1024 and out < 2048:
        return str(round(float(out) / 1024.0, 1)) + 'G'
    return str(out / 1024) + 'G'

def partitionVersion(ver):
    return [int(part) if part.isdigit() else part
                      for part in ver.split('.')]

def adjustInfo(info):
    for channel in ('release', 'beta', 'aurora', 'nightly'):
        if channel in info['appUpdateChannel'].lower():
            info['appUpdateChannel'] = channel
            break

    if ('memsize' in info and
        str(info['memsize']).isdigit() and
        int(info['memsize']) > 0):
        info['memsize'] = roundMemSize(int(info['memsize']))
    else:
        info['memsize'] = None

    if info.get('appName') == 'B2G':
        info['OS'] = 'B2G'

    if 'version' in info and 'OS' in info:
        info['os'] = (str(info['OS']) + ' ' +
            '.'.join(str(info['version']).split('-')[0].split('.')[:2]))
    elif 'OS' in info:
        info['os'] = str(info['OS'])
    else:
        info['os'] = None

    if ('cpucount' in info and
        str(info['cpucount']).isdigit() and
        int(info['cpucount']) > 0):
        info['cpucount'] = int(info['cpucount'])
    else:
        info['cpucount'] = None

    if 'OS' in info:
        info['platform'] = info['OS']
    else:
        info['platform'] = None

    if ('adapterRAM' in info and
        str(info['adapterRAM']).isdigit() and
        int(info['adapterRAM']) > 0):
        info['adapterRAM'] = roundMemSize(int(info['adapterRAM']))
    else:
        info['adapterRAM'] = None

    if 'arch' in info:
        arch = info['arch']
        if 'arm' in arch:
            info['arch'] = ('armv7'
                if ('v7' in arch or info.get('hasARMv7', 'v6' not in arch))
                else 'armv6')

    if 'appBuildID' in info and 'appVersion' in info:
        info['appBuildID'] = info['appVersion'] + '-' + info['appBuildID']

def filterInfo(raw_info):
    adjustInfo(raw_info)
    return {k: (raw_info[k]
                if (k in raw_info and raw_info[k] is not None)
                else 'unknown')
            for k in allowed_infos}
    # return {k: v for k, v in raw_info.iteritems()
    #         if k in allowed_infos}

def filterDimensions(raw_dims, raw_info):
    return {dim: (raw_dims[dimensions.index(dim)]
                  if dim not in raw_info else raw_info[dim])
            for dim in allowed_dimensions}

def estQuantile(values, n, key=lambda x:x):
    histograms = {}
    offset = 1 - key(min(values, key=key))
    for x in (key(v) for v in values):
        k = round(math.log(x + offset), 2)
        histograms[k] = histograms.get(k, 0) + 1
    def _est(keys):
        need = len(values) / n
        for k in keys:
            count = histograms[k]
            if need <= count:
                return math.exp(k + 0.01 * (1.0 -
                    float(need) / float(count))) - offset
            need -= count
    keys = list(histograms.iterkeys())
    keys.sort()
    return (_est(keys), _est(reversed(keys)))

def quantile(values, n, upper=True, key=lambda x:x):
    maxs = [key(x) for x in values[: len(values) / n]]
    curidx, curmin = (min(enumerate(maxs), key=lambda x:x[1]) if upper else
                      max(enumerate(maxs), key=lambda x:x[1]))
    if not len(maxs):
        return max(values) if upper else min(values)
    for v in (key(x) for x in values[len(values) / n:]):
        if ((upper and v <= curmin) or
            (not upper and v >= curmin)):
            continue
        maxs[curidx] = v
        curidx, curmin = (min(enumerate(maxs), key=lambda x:x[1]) if upper else
                          max(enumerate(maxs), key=lambda x:x[1]))
    return curmin
