# -*- coding: utf-8
'''
Plots the distribution of the fraction of time, considering the whole
lifespan of a video, which took for each group of referrer to appear.
'''
from __future__ import division, print_function

from collections import defaultdict

from vod.fileutil import write_stats_to_file
from vod.fileutil import write_xy_to_file
from vod.mapreducescript import BaseReducer
from vod.stats.curves import ecdf

from vodstats.plot.plot2d import box_plot, xy_plot, Box, XYPoints

from youtime import YoutimeH5Runner
from youtime.common.dao import VideoDAO
from youtime.event_dates import EventDateMapper
from youtime.mapred.ig import PyTablesDaoIterator

import os
import sys
        
class EventDatesGroupedReducer(BaseReducer):

    def __init__(self, outf):
        self.outf = outf
        self.vals = defaultdict(list)
        
    def _reduce(self, k, v):
        groups = VideoDAO.EV2GROUP
        for e, data in v.items():
            self.vals[groups[e]].extend(data)
        
    def close(self):
        groups = VideoDAO.EV_GROUPS.copy()
        del groups['NOT_CAPTURED']
        
        styles = ['lightgrey'] * 7
        stylemap = dict( zip(sorted(groups), styles) )

        boxes = []
        lines = []
        for group in sorted(groups):
            group_cdf = ecdf(self.vals[group])
            line = XYPoints(group_cdf[0], group_cdf[1], group)
            box = Box(self.vals[group], group, stylemap[group])

            lines.append(line)
            boxes.append(box)
            
            write_stats_to_file(self.vals[group], 
                                os.path.join(self.outf, group+'-time.stats'))
            write_xy_to_file(group_cdf[0], group_cdf[1], 
                             os.path.join(self.outf, group+'-time.cdf'))
        
        xy_plot(*lines,
                  xlabel='Fraction of days since upload', 
                  ylabel='Prob. (Fraction of days since upload $\leq$ x)',
                  legborder=False,
                  legloc='lower right',
                  xmin=0.0, xmax=1.0,
                  ymin=0.0, ymax=1.0,
                  outputf=os.path.join(self.outf, 'evtime-grouped-cdf.png'))
        
        box_plot(*boxes,
                  xlabel='Referrer Category', 
                  ylabel='Time Until First Referral (\% lifetime)',
                  ymin=-0.05, ymax=1.05, grid=False,
                  legborder=False, xmin=0.5, xmax=len(boxes) + 0.5,
                  outputf=os.path.join(self.outf, 'evtime-grouped-box.png'))
        
class EventDateGroupedRunner(YoutimeH5Runner):
    
    def __init__(self, name, description):
        super(EventDateGroupedRunner, self).__init__(name, description)
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
        self.reducer_obj = EventDatesGroupedReducer(arg_vals.outf)

    def finalize(self):
        self.reducer_obj.close()
        
if __name__ == '__main__':
    runner = EventDateGroupedRunner(sys.argv[0], __doc__)
    runner(sys.argv[1:])