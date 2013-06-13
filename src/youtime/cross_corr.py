# -*- coding: utf-8
'''
Cross Correlates all curves for each video.
'''
from __future__ import division, print_function

from matplotlib import pyplot as plt

from scipy import stats

from vod.mapreducescript import BaseMapper
from vod.mapreducescript import BaseReducer

from youtime import YoutimeH5Runner
from youtime.common.constants import TSERIES_SIZE
from youtime.common.time_series import cross_corr
from youtime.common.dao import VideoDAO
from youtime.mapred.ig import PyTablesDaoIterator

import numpy as np
import os
import sys

class CrossCorrelateMapper(BaseMapper):

    def _map(self, key, item):
        
        if (item[VideoDAO.VIEW_DATA_ORIG] > TSERIES_SIZE):
            view = item[VideoDAO.VIEW_DATA_ORIG]
            comm = item[VideoDAO.COMM_DATA_ORIG]
            favs = item[VideoDAO.FAVS_DATA_ORIG]
            
            return_val = np.ndarray(shape=(6, TSERIES_SIZE), dtype='f')
            for i in range(TSERIES_SIZE):
                return_val[0, i] = cross_corr(view, comm)
                return_val[1, i] = cross_corr(view, favs)
                return_val[2, i] = cross_corr(comm, favs)
                
                return_val[3, i] = cross_corr(view, view)
                return_val[4, i] = cross_corr(comm, comm)
                return_val[5, i] = cross_corr(favs, favs)

            return return_val
            
class CrossCorrelateReducer(BaseReducer):

    def __init__(self, outf):
        super(CrossCorrelateReducer, self).__init__()
        
        self.outf = outf
        
        self.view_comm = []
        self.view_favs = []
        self.comm_favs = []
        
        self.view_view = []
        self.comm_comm = []
        self.favs_favs = []
        
    def _reduce(self, key, value):
        
        if not value:
            return
        
        self.view_comm.append(value[0])
        self.view_favs.append(value[1])
        self.comm_favs.append(value[2])
        self.view_view.append(value[3])
        self.comm_comm.append(value[4])
        self.favs_favs.append(value[5])
            
    def close(self):
        
        view_comm = np.asarray(self.view_comm).T
        view_favs = np.asarray(self.view_favs).T
        comm_favs = np.asarray(self.comm_favs).T

        view_view = np.asarray(self.view_view).T
        comm_comm = np.asarray(self.comm_comm).T
        favs_favs = np.asarray(self.favs_favs).T
        
        view_comm_gauss = stats.kde.gaussian_kde(view_comm)
        view_favs_gauss = stats.kde.gaussian_kde(view_favs)
        comm_favs_gauss = stats.kde.gaussian_kde(comm_favs)
        
        view_view_gauss = stats.kde.gaussian_kde(view_view)
        comm_comm_gauss = stats.kde.gaussian_kde(comm_comm)
        favs_favs_gauss = stats.kde.gaussian_kde(favs_favs)
        
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