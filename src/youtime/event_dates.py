# -*- coding: utf-8
'''
Plots the distribution of the fraction of time, considering the whole
lifespan of a video, which took for referrers to appear.
'''
from __future__ import division, print_function

from collections import defaultdict

from vod.fileutil import write_stats_to_file
from vod.fileutil import write_xy_to_file
from vod.mapreducescript import BaseMapper
from vod.mapreducescript import BaseReducer
from vod.stats.curves import ecdf

from vodstats.plot.plot2d import xy_plot, XYPoints

from youtime import YoutimeH5Runner
from youtime.common.dao import VideoDAO
from youtime.mapred.ig import PyTablesDaoIterator

import os
import sys

class EventDateMapper(BaseMapper):

    def _map(self, key, item):
        dates  = item[VideoDAO.EVENT_DATES]
        typez  = item[VideoDAO.EVENT_TYPES]
        points = item[VideoDAO.DATE_POINTS_INTERP]

        valid_dates = {}
        for i in xrange(len(points)):
            valid_dates[points[i]] = i

        return_val = defaultdict(list)
        for ev_date, ev_type in zip(dates, typez):
            frac = valid_dates[ev_date] / len(points)
            return_val[ev_type].append(frac)
            
        return return_val

class EventDateReducer(BaseReducer):

    def __init__(self, outf):
        self.outf = outf
        self.vals = []
        
    def _reduce(self, key, value):
        for ev_type in value:
            self.vals.extend(value[ev_type])
        
    def close(self):
        ecdf_points = ecdf(self.vals)
        line = XYPoints(ecdf_points[0], ecdf_points[1], 'All Events', 'k-')
        
        xy_plot(line, xlabel='Fraction of days since upload', 
                ylabel='Prob. (Fraction of days since upload $\leq$ x)',
                outputf=os.path.join(self.outf, 'evtime-cdf.png'),
                legloc = 'lower right', legborder = False, 
                xmin=0.0, xmax=1.0, ymin=0.0, ymax=1.0)
        
        write_stats_to_file(self.vals, os.path.join(self.outf, 'evtime.stats'))
        write_xy_to_file(ecdf_points[0], ecdf_points[1], 
                         os.path.join(self.outf, 'evtime-percent.cdf'))
        
class EventDatesRunner(YoutimeH5Runner):

    def __init__(self, name, description):
        super(EventDatesRunner, self).__init__(name, description)
        self.reducer_obj = None
        self.mapper_obj = None
    
    def item_generator(self):
        return self.igen
    
    def mapper(self):
        return self.mapper_obj
    
    def reducer(self):
        return self.reducer_obj
    
    def setup(self, arg_vals):
        self.igen = PyTablesDaoIterator(arg_vals.in_file, arg_vals.table)
        self.mapper_obj = EventDateMapper()
        self.reducer_obj = EventDateReducer(arg_vals.outf)
    
    def finalize(self):
        self.reducer_obj.close()
        
if __name__ == '__main__':
    runner = EventDatesRunner(sys.argv[0], __doc__)
    runner(sys.argv[1:])