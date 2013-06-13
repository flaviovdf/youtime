# -*- coding: utf-8
'''
Does s scatter plot of the amount of views a video receive by it's
total time (number of days).
'''
from __future__ import division, print_function

from scipy.stats import pearsonr

from vod.mapreducescript import BaseReducer

from vodstats.plot.plot2d import xy_plot, XYPoints

from youtime import YoutimeH5Runner
from youtime.common.dao import VideoDAO
from youtime.mapred.ig import PyTablesDaoIterator
from youtime.mapred.mappers import Noop

import os
import sys

class ViewsXTimeReducer(BaseReducer):

    def __init__(self, outf):
        assert(os.path.isdir(outf))
        self.outf = outf
        self.view = []
        self.days = []
    
    def _reduce(self, key, value):
        self.view.append(value[VideoDAO.TOTAL_VIEW])
        self.days.append(len(value[VideoDAO.DATE_POINTS_INTERP]))

    def close(self):
        corr =  pearsonr(self.view, self.days)
        xy_plot(XYPoints(self.view, self.days, style='bo'), 
                xlabel='Views', ylabel='Days', 
                title='Views x Days (%.2f , %.2f)' % corr, 
                outputf=os.path.join(self.outf, 'viewsxtime.png'), 
                logx=True, logy=True)
        
class ViewsXTime(YoutimeH5Runner):

    def __init__(self, name, description):
        super(ViewsXTime, self).__init__(name, description)
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
        self.mapper_obj = Noop()
        self.reducer_obj = ViewsXTimeReducer(arg_vals.outf)
        
    def finalize(self):
        self.reducer_obj.close()
        
if __name__ == '__main__':
    runner = ViewsXTime(sys.argv[0], __doc__)
    runner(sys.argv[1:])