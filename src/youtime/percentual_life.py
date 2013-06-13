# -*- coding: utf-8
'''
Plots the CDF of the fraction of time it take's for videos to 
receive 10%, 90% and 50% of their final views.
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

DAY = 86400
class PercentualLifeMapper(BaseMapper):
    
    def __init__(self, cname):
        if cname == 'view':
            self.dname = VideoDAO.VIEW_DATA_INTERP
            self.tname = VideoDAO.TOTAL_VIEW
            self.aname = 'views'
        elif cname == 'comm':
            self.dname = VideoDAO.COMM_DATA_INTERP
            self.tname = VideoDAO.TOTAL_COMM
            self.aname = 'comments'
        elif cname == 'favs':
            self.dname = VideoDAO.FAVS_DATA_INTERP
            self.tname = VideoDAO.TOTAL_FAVS    
            self.aname = 'favorites'
        else:
            raise Exception()
        
        self.cname = cname
        
    def _map(self, key, item):
        statistics = item[self.dname]
        total = item[self.tname]
        total_days = len(statistics)
        
        if total == 0.0:
            return None

        rv10 = 1
        rv50 = 1
        rv90 = 1

        sum_ = 0.0
        for i in xrange(len(statistics)):
            sum_ += statistics[i]
            if self.cname != 'avgr':
                perc = (100.0*sum_)/total
            else:
                perc = (100.0*(sum_/i))/total if i > 0 else 0
                
            key = (1.0*i)/total_days
            
            if perc >= 10 and rv10 == 1:
                rv10 = key
            
            if perc >= 50 and rv50 == 1:
                rv50 = key
            
            if perc >= 90 and rv90 == 1:
                rv90 = key
        
        return rv10, rv50, rv90

class PecentualLifeReducer(BaseReducer):
        
    def __init__(self, outf, cname):
        assert(os.path.isdir(outf))
        self.outf = outf
        self.cname = cname
        self.data = defaultdict(list)
        if cname == 'view':
            self.aname = 'views'
        elif cname == 'comm':
            self.aname = 'comments'
        elif cname == 'favs':
            self.aname = 'favorites'
        else:
            raise Exception()
        
        self.scores10 = []
        self.scores50 = []
        self.scores90 = []
        
    def _reduce(self, key, value):
        if value:
            self.scores10.append(value[0])
            self.scores50.append(value[1])
            self.scores90.append(value[2])
    
    def close(self):
        scores10cdf = ecdf(self.scores10)
        scores50cdf = ecdf(self.scores50)
        scores90cdf = ecdf(self.scores90)
        
        line10 = XYPoints(scores10cdf[0], scores10cdf[1], '', 'k:')
        line50 = XYPoints(scores50cdf[0], scores50cdf[1], '', 'r')
        line90 = XYPoints(scores90cdf[0], scores90cdf[1], '', 'k-.')
        
        xy_plot(line10, line50, line90, 
                 xlabel=r'Fraction of days since upload - $f$', 
                 ylabel=r'Prob. (Fraction of days since upload $\leq$ f)', 
                 legloc = 'lower center', legborder = False, legend=True,
                 xmin=0.0, xmax=1.0,
                 ymin=0.0, ymax=1.0,
                 outputf=os.path.join(self.outf, 
                                      self.cname+'-105090-cdf.png')) 
        
        write_stats_to_file(self.scores10, 
                            os.path.join(self.outf, 
                                         self.cname+'-10percent.stats'))
        write_stats_to_file(self.scores50, 
                            os.path.join(self.outf, 
                                         self.cname+'-50percent.stats'))
        write_stats_to_file(self.scores90, 
                            os.path.join(self.outf, 
                                         self.cname+'-90percent.stats'))
        
        write_xy_to_file(scores10cdf[0], scores10cdf[1], 
                         os.path.join(self.outf, self.cname+'-10percent.cdf'))
        write_xy_to_file(scores50cdf[0], scores50cdf[1], 
                         os.path.join(self.outf, self.cname+'-50percent.cdf'))
        write_xy_to_file(scores90cdf[0], scores90cdf[1], 
                         os.path.join(self.outf, self.cname+'-90percent.cdf')) 
        
class PercentualLife(YoutimeH5Runner):

    def __init__(self, name, description):
        super(PercentualLife, self).__init__(name, description)
        self.mapper_obj = None
        self.reducer_obj = None
    
    def item_generator(self):
        return self.igen
    
    def mapper(self):
        return self.mapper_obj
    
    def reducer(self):
        return self.reducer_obj
    
    def add_custom_aguments(self, parser):
        super(PercentualLife, self).add_custom_aguments(parser)
        parser.add_argument('cname', type=str, help='Column Name')
        
    def setup(self, arg_vals):
        self.igen = PyTablesDaoIterator(arg_vals.in_file, arg_vals.table)
        self.mapper_obj = PercentualLifeMapper(arg_vals.cname)
        self.reducer_obj = PecentualLifeReducer(arg_vals.outf, arg_vals.cname)
    
    def finalize(self):
        self.reducer_obj.close()
    
if __name__ == '__main__':
    runner = PercentualLife(sys.argv[0], __doc__)
    runner(sys.argv[1:])