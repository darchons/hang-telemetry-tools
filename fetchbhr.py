#!/usr/bin/env python2

if __name__ == '__main__':

    import os, shutil, sys, tempfile
    import simplejson as json
    from datetime import datetime, timedelta
    from fetchanr import processBHR, runJob

    if len(sys.argv) != 3:
        print 'Usage %s <from> <to>' % (sys.argv[0])
        sys.exit(1)

    DATE_FORMAT = '%Y%m%d'
    fromDate = datetime.strptime(sys.argv[1], DATE_FORMAT)
    toDate = datetime.strptime(sys.argv[2], DATE_FORMAT)

    if toDate < fromDate:
        print 'To date is less than from date'
        sys.exit(1)

    mindate = fromDate.strftime(DATE_FORMAT)
    maxdate = toDate.strftime(DATE_FORMAT)
    workdir = os.path.join('/mnt', 'tmp-bhr-%s-%s' % (mindate, maxdate))
    localonly = os.path.exists(os.path.join(workdir, 'cache'))
    if not os.path.exists(workdir):
        os.makedirs(workdir)

    outdir = os.path.join('/mnt', 'bhr-%s-%s' % (mindate, maxdate))
    if not os.path.exists(outdir):
        os.makedirs(outdir)

    print 'Range: %s to %s' % (mindate, maxdate)
    print 'Work dir: %s' % workdir
    print 'Out dir: %s' % outdir
    if localonly:
        print 'Local only'

    dims = [{
        'field_name': 'reason',
        'allowed_values': ['saved-session']
    }, {
        'field_name': 'appName',
        'allowed_values': [
            'B2G',
            'Fennec',
            'Firefox',
            'Thunderbird',
            'Webapp Runtime',
            'MetroFirefox',
        ]
    }, {
        'field_name': 'appUpdateChannel',
        'allowed_values': [
            'nightly',
            'aurora'
        ]
    }, {
        'field_name': 'appVersion',
        'allowed_values': '*'
    }, {
        'field_name': 'appBuildID',
        'allowed_values': '*'
    }, {
        'field_name': 'submission_date',
        'allowed_values': {
            'min': mindate,
            'max': maxdate
        }
    }]

    index = {
        'dimensions': {},
        'sessions': {},
    }

    summaryout = os.path.join(outdir, 'summary.txt')
    runJob("mapreduce-bhr-summary.py", dims, workdir,
           summaryout, local=localonly)
    shutil.copyfile(summaryout, 'summary.txt')

    filterout = os.path.join(outdir, 'filter.txt')
    runJob("mapreduce-bhr-filter.py", dims, workdir,
           filterout, local=True)
    shutil.copyfile(filterout, 'filter.txt')

    with tempfile.NamedTemporaryFile('r', suffix='.txt', dir=workdir) as outfile:
        runJob("mapreduce-bhr.py", dims, workdir, outfile.name, local=True)
        with open(outfile.name, 'r') as jobfile:
            processBHR(index, jobfile, outdir)

    with open(os.path.join(outdir, 'index.json'), 'w') as outfile:
        outfile.write(json.dumps(index, separators=(',', ':')))

    print 'Completed'

