# -*- coding: utf-8
'''
Plots the distribution of the fraction of views which referrers from each group 
were responsible for.
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
from youtime.event_views import EventViewsMapper
from youtime.mapred.ig import PyTablesDaoIterator

import os
import sys

class EventViewsReducer(BaseReducer):

    def __init__(self, outf):
        self.outf = outf
        self.vals = defaultdict(list)
        
    def _reduce(self, key, value):
        for t in value:
            self.vals[t].extend(value[t])
        
    def close(self):
        ev2group = VideoDAO.EV2GROUP
        groups = VideoDAO.EV_GROUPS
        del groups['NOT_CAPTURED']

        vals_per_group = defaultdict(list)
        
        for t in self.vals:
            values = self.vals[t]
            vals_per_group[ev2group[t]].extend(values)
        
        styles = ['lightgrey'] * 7
        stylemap = dict(zip(sorted(groups), styles))

        boxes = []
        lines = []
        for group in sorted(groups):
            group_cdf = ecdf(vals_per_group[group])
            line = XYPoints(group_cdf[0], group_cdf[1], group)
            box = Box(vals_per_group[group], group, stylemap[group])

            lines.append(line)
            boxes.append(box)
            
            write_stats_to_file(vals_per_group[group], 
                                os.path.join(self.outf, group+'-views.stats'))
            write_xy_to_file(group_cdf[0], group_cdf[1], 
                             os.path.join(self.outf, group+'-views.cdf'))
        
        box_plot(*boxes,
                  xlabel='Referrer Category', ylabel='Fraction of Views',
                  ymin=-0.05, ymax=1.05, grid=False,
                  legborder=False, xmin=0.5, xmax=len(boxes) + 0.5, 
                  xrotation=20,
                  outputf=os.path.join(self.outf, 'ev-grouped-views-box.png'))
        
        xy_plot(*lines,
                  xlabel='Fraction of Views', ylabel='Prob. (Fraction of Views $\leq$ x)',
                  grid=False,
                  legborder=False,
                  xmin=0.0, xmax=1.0,
                  ymin=0.0, ymax=1.0,
                  logx=False, logy=True,
                  outputf=os.path.join(self.outf, 'ev-grouped-views-cdf.png'))
        
class EventViewsGroupedRunner(YoutimeH5Runner):

    def __init__(self, name, description):
        super(EventViewsGroupedRunner, self).__init__(name, description)
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
    runner = EventViewsGroupedRunner(sys.argv[0], __doc__)
    runner(sys.argv[1:])