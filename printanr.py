#!/usr/bin/env python2

if __name__ == '__main__':

    import json, os, sys
    from anr import ANRReport

    if len(sys.argv) != 3:
        print 'Usage %s <file> <lines>' % (sys.argv[0])
        sys.exit(1)

    lines = [int(l, 0) for l in sys.argv[2].split(',')]

    with open(sys.argv[1], 'r') as f:
        line = -1
        for l in f:
            line += 1
            if line not in lines:
                continue
            anr = ANRReport(l)
            traces = anr.rawData.pop('androidANR', None)
            logcat = anr.rawData.pop('androidLogcat', None)
            print '===== ANR file %s line %d =====' % (
                os.path.basename(sys.argv[1]), line)
            print json.dumps(anr.rawData, indent=4)
            print '===== raw traces ====='
            print traces
            print '===== end raw traces ====='
            print '===== raw logcat ====='
            print logcat
            print '===== end raw logcat ====='
            print '===== END ANR ====='
            print

