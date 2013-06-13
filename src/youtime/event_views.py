# -*- coding: utf-8
'''
Plots the distribution of the fraction of views which referrers 
from each type were responsible for.
'''
from __future__ import division, print_function

from collections import defaultdict
from itertools import izip

from scipy.stats import scoreatpercentile

from vod.mapreducescript import BaseMapper
from vod.mapreducescript import BaseReducer

from youtime import YoutimeH5Runner
from youtime.mapred.ig import PyTablesDaoIterator
from youtime.common.dao import VideoDAO

import numpy as np
import os
import sys

class EventViewsMapper(BaseMapper):

    def _map(self, key, item):
        views = item[VideoDAO.EVENT_VIEWS]
        types = item[VideoDAO.EVENT_TYPES]
        total = item[VideoDAO.TOTAL_VIEW]

        sumev = sum(views)
        return_val = defaultdict(list)
        if total > 0:
            for typez, view in izip(types, views):
                frac = view / sumev
                assert frac <= 1.0
                return_val[typez].append(frac)

            residual = (total - sumev) / total
            if residual >= 0:
                return_val['NOT_CAPTURED'].append(residual)
            else:
                return_val['NOT_CAPTURED'].append(0)

        return return_val
        
class EventViewsReducer(BaseReducer):

    def __init__(self, outf):
        self.outf = outf
        self.vals = defaultdict(list)
        
    def _reduce(self, key, value):
        for t in value:
            self.vals[t].extend(value[t])
        
    def close(self):
        out_fpath = os.path.join(self.outf, 'events-fractions.stats')
        with open(out_fpath, 'w') as out_file:
            for typez in self.vals:
                mean = np.mean(self.vals[typez])
                std = np.std(self.vals[typez])
                
                s10 = scoreatpercentile(self.vals[typez], 10)
                s50 = scoreatpercentile(self.vals[typez], 50)
                s90 = scoreatpercentile(self.vals[typez], 90)
                
                print(typez, file=out_file)
                print('\t', 'mean: ', mean, file=out_file)
                print('\t', 'std: ', std, file=out_file)
                print('\t', 'max: ', max(self.vals[type]), file=out_file)
                print('\t', 'per_10: ', s10, file=out_file)
                print('\t', 'per_50: ', s50, file=out_file)
                print('\t', 'per_90: ', s90, file=out_file)
                print('', file=out_file)
                print('==========', file=out_file)
                print('', file=out_file)
            
        
class EventViewsRunner(YoutimeH5Runner):

    def __init__(self, name, description):
        super(EventViewsRunner, self).__init__(name, description)
        self.mapper_obj = None
        self.reducer_obj = None
    
    def item_generator(self):
        return self.igen
    
    def mapper(self):
        return self.mapper_obj
    
    def reducer(self):
        return self.reducer_obj
    
    def setup(self, arg_vals):
        self.igen = PyTablesDaoIterator(arg_vals.in_file, arg_vals.table)
        self.mapper_obj = EventViewsMapper()
        self.reducer_obj = EventViewsReducer(arg_vals.outf)
    
    def finalize(self):
        self.reducer_obj.close()
    
if __name__ == '__main__':
    runner = EventViewsRunner(sys.argv[0], __doc__)
    runner(sys.argv[1:])