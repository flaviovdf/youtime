# -*- coding: utf-8
'''
Correlates all curves (time series arrays) and plots CDF.
'''
from __future__ import division, print_function

from scipy.stats import pearsonr

from vod.fileutil import write_stats_to_file
from vod.mapreducescript import BaseMapper
from vod.mapreducescript import BaseReducer
from vod.stats.curves import ecdf

from vodstats.plot.plot2d import xy_plot, XYPoints

from youtime import YoutimeH5Runner
from youtime.common.dao import VideoDAO
from youtime.mapred.ig import PyTablesDaoIterator

import numpy as np
import os
import sys

class CorrelateMapper(BaseMapper):

    def _map(self, key, item):
        view = item[VideoDAO.VIEW_DATA_INTERP]
        comm = item[VideoDAO.COMM_DATA_INTERP]
        favs = item[VideoDAO.FAVS_DATA_INTERP]
        
        return (pearsonr(view, comm)[0],
                pearsonr(view, favs)[0],
                pearsonr(comm, favs)[0])
        
class CorrelateReducer(BaseReducer):

    def __init__(self, outf):
        super(CorrelateReducer, self).__init__()
        
        self.outf = outf
        self.view_comm = []
        self.view_favs = []
        self.comm_favs = []
        
    def _reduce(self, key, value):
        point = value[0]
        if not np.isnan(point):
            self.view_comm.append(point)
        
        point = value[1]
        if not np.isnan(point):
            self.view_favs.append(point)
        
        point = value[2]
        if not np.isnan(point):
            self.comm_favs.append(point)
            
    def close(self):
        view_comm_cdf = ecdf(self.view_comm)
        view_favs_cdf = ecdf(self.view_favs)
        comm_favs_cdf = ecdf(self.comm_favs)
        
        view_comm_cdf_line = XYPoints(view_comm_cdf[0], view_comm_cdf[1], 
                                      'Views x Comments')
        view_favs_cdf_line = XYPoints(view_favs_cdf[0], view_favs_cdf[1], 
                                      'Views x Favorites')
        comm_favs_cdf_line = XYPoints(comm_favs_cdf[0], comm_favs_cdf[1], 
                                      'Comments x Favorites')
        
        xy_plot(view_comm_cdf_line, view_favs_cdf_line, comm_favs_cdf_line, 
                xlabel=r'Pearson Correlation ($\rho$)', 
                ylabel=r'Prob. ($\rho \leq x$)', 
                outputf=os.path.join(self.outf, 'corr-vc-vf-vr-cdf.png'))
        
        xy_plot(view_comm_cdf_line, 
                xlabel=r'Pearson Correlation ($\rho$)', 
                ylabel=r'Prob. ($\rho \leq x$)', 
                outputf=os.path.join(self.outf, 'corr-view-comm-cdf.png'))
        xy_plot(view_favs_cdf_line, 
                xlabel=r'Pearson Correlation ($\rho$)', 
                ylabel=r'Prob. ($\rho \leq x$)', 
                outputf=os.path.join(self.outf, 'corr-view-favs-cdf.png'))
        xy_plot(comm_favs_cdf_line, 
                xlabel=r'Pearson Correlation ($\rho$)', 
                ylabel=r'Prob. ($\rho \leq x$)', 
                outputf=os.path.join(self.outf, 'corr-comm-favs-cdf.png'))
        
        write_stats_to_file(self.view_comm, 
                            os.path.join(self.outf, 'corr-view-comm.stats'))
        write_stats_to_file(self.view_favs, 
                            os.path.join(self.outf, 'corr-view-favs.stats'))
        write_stats_to_file(self.comm_favs, 
                            os.path.join(self.outf, 'corr-comm-favs.stats'))
        
class Correlate(YoutimeH5Runner):

    def __init__(self, name, description):
        super(Correlate, self).__init__(name, description)
        self.mapper_obj = None
        self.reducer_obj = None
    
    def item_generator(self):
        return self.igen
    
    def mapper(self):
        return self.mapper_obj
    
    def reducer(self):
        return self.reducer_obj
    
    def setup(self, arg_vals):
        self.mapper_obj = CorrelateMapper()
        self.reducer_obj = CorrelateReducer(arg_vals.outf)
        self.igen = PyTablesDaoIterator(arg_vals.in_file, arg_vals.table)
    
    def finalize(self):
        self.reducer_obj.close()
    
if __name__ == '__main__':
    runner = Correlate(sys.argv[0], __doc__)
    runner(sys.argv[1:])