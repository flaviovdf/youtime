# -*- coding: utf-8
'''
Computes the CDF of a numeric column from the H5 database.
'''
from __future__ import division, print_function

from vod.fileutil import write_stats_to_file
from vod.fileutil import write_xy_to_file
from vod.mapreducescript import BaseReducer
from vod.stats.curves import ecdf

from vodstats.plot.plot2d import xy_plot, XYPoints

from youtime import YoutimeH5Runner
from youtime.common.dao import VideoDAO
from youtime.mapred.ig import PyTablesDaoIterator
from youtime.mapred.mappers import Noop

import os
import sys

class CDFColReducer(BaseReducer):
    
    def __init__(self, outf, cname, logx=False, logy=False):
        assert(os.path.isdir(outf))
        self.outf = outf
        self.logx = logx
        self.logy = logy
        self.data = []
        
        if cname == 'view':
            self.dname = VideoDAO.VIEW_DATA_INTERP
            self.tname = VideoDAO.TOTAL_VIEW
            self.curve_label = 'Total number of views'
        elif cname == 'comm':
            self.dname = VideoDAO.COMM_DATA_INTERP
            self.tname = VideoDAO.TOTAL_COMM
            self.curve_label = 'Total number of comments'
        elif cname == 'favs':
            self.dname = VideoDAO.FAVS_DATA_INTERP
            self.tname = VideoDAO.TOTAL_FAVS
            self.curve_label = 'Total number of favorites'    
        else:
            raise Exception()
        
        self.cname = cname
        
    def _reduce(self, key, value):
        point = value[self.tname]
        self.data.append(point)

    def close(self):
        dat_cdf = ecdf(self.data)
        
        cdf_line = XYPoints(dat_cdf[0], dat_cdf[1], name=self.curve_label)
        xy_plot(cdf_line,
                 xlabel=self.curve_label, 
                 ylabel='Prob. ('+self.curve_label+' $\leq$ x)', 
                 outputf=os.path.join(self.outf, self.cname+'-cdf.png'), 
                 logx=self.logx, logy=self.logy)
        
        write_stats_to_file(self.data, 
                            os.path.join(self.outf, self.cname+'.dat'))
        write_xy_to_file(dat_cdf[0], dat_cdf[1], 
                         os.path.join(self.outf, self.cname+'.cdf'))

class CDFRunner(YoutimeH5Runner):
    
    def __init__(self, name, description):
        super(CDFRunner, self).__init__(name, description)
        self.mapper_obj = None
        self.reducer_obj = None
    
    def item_generator(self):
        return self.igen
    
    def mapper(self):
        return self.mapper_obj
    
    def reducer(self):
        return self.reducer_obj
    
    def add_custom_aguments(self, parser):
        super(CDFRunner, self).add_custom_aguments(parser)
        parser.add_option('column_name', type=str, 
                          help='Column Name')
        parser.add_option('--logx',  action='store_true', 
                          help='Log x scale')
        parser.add_option('--logy',  action='store_true', 
                          help='Log y scale')
        
    def setup(self, arg_vals):
        self.mapper_obj = Noop()
        self.reducer_obj = CDFColReducer(arg_vals.outf, arg_vals.cname, 
                                         arg_vals.logx, arg_vals.logy)
        self.igen = PyTablesDaoIterator(arg_vals.in_file, arg_vals.table)
    
    def finalize(self):
        self.reducer.close()
        
if __name__ == '__main__':
    runner = CDFRunner(sys.argv[0], __doc__)
    runner(sys.argv[1:])