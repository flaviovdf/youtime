#!/usr/bin/env python
# -*- coding: utf-8
from __future__ import division

import sys, time

inf = sys.argv[1]

with open(inf) as f:
    for l in f:
        spl = l.split()
        if spl[7] == 'NULL':
            continue
        
        if not 'down:' in l:
            continue
        
        vid_id = spl[2]
        up_date = time.mktime(time.strptime(spl[7], '%Y-%m-%d'))
        for i in xrange(15, len(spl) - 1):
            try:
                del_date = time.mktime(time.strptime(spl[15], '%Y-%m-%d'))
                break
            except:
                pass
            
        print vid_id, up_date, del_date
