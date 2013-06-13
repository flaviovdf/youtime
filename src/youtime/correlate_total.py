# -*- coding: utf-8
'''
Correlates all pairs of numeric columns from the H5 database
'''
from __future__ import division, print_function

from scipy.stats import pearsonr

from vod.fileutil import write_xy_to_file
from vod.mapreducescript import BaseReducer

from vodstats.plot.plot2d import xy_plot, XYPoints

from youtime import YoutimeH5Runner
from youtime.common.dao import VideoDAO
from youtime.mapred.ig import PyTablesDaoIterator
from youtime.mapred.mappers import Noop

import os
import sys

class CorrelateTotalReducer(BaseReducer):

    def __init__(self, outf):
        assert(os.path.isdir(outf))
        
        self.outf = outf
        self.view = []
        self.comm = []
        self.favs = []
        self.nevents = []
    
    def _reduce(self, key, value):
        self.view.append(value[VideoDAO.TOTAL_VIEW])
        self.comm.append(value[VideoDAO.TOTAL_COMM])
        self.favs.append(value[VideoDAO.TOTAL_FAVS])
        self.nevents.append(len(value[VideoDAO.EVENT_DATES]))

    def close(self):
        view_comm =  pearsonr(self.view, self.comm)
        view_favs =  pearsonr(self.view, self.favs)
        comm_favs =  pearsonr(self.comm, self.favs)
        
        view_events =  pearsonr(self.view, self.nevents)
        comm_events =  pearsonr(self.comm, self.nevents)
        favs_events =  pearsonr(self.favs, self.nevents)
        
        xy_plot(XYPoints(self.view, self.comm, style='bo'), 
                xlabel='Views', ylabel='Comments', 
                title='Views x Comments (%.2f , %.2f)'%view_comm, 
                outputf=os.path.join(self.outf, 'total-viewsxcomm.png'), 
                logx=True, logy=True)
        
        xy_plot(XYPoints(self.view, self.favs, style='bo'), 
                xlabel='Views', ylabel='Favorites', 
                title='Views x Favorites (%.2f , %.2f)'%view_favs, 
                outputf=os.path.join(self.outf, 'total-viewsxfavs.png'), 
                logx=True, logy=True)
        
        xy_plot(XYPoints(self.comm, self.favs, style='bo'), 
                xlabel='Comments', ylabel='Favorites', 
                title='Comments x Favorites (%.2f , %.2f)'%comm_favs, 
                outputf=os.path.join(self.outf, 'total-commxfavs.png'), 
                logx=True, logy=True)
        
        xy_plot(XYPoints(self.view, self.nevents, style='bo'), 
                xlabel='Views', ylabel='Number of Events', 
                title='Views x Events (%.2f , %.2f)'%view_events, 
                outputf=os.path.join(self.outf, 'total-viewxevts.png'), 
                logx=True, logy=False)
        
        xy_plot(XYPoints(self.comm, self.nevents, style='bo'), 
                xlabel='Comments', ylabel='Number of Events', 
                title='Comments x Events (%.2f , %.2f)'%comm_events, 
                outputf=os.path.join(self.outf, 'total-commxevts.png'), 
                logx=True, logy=False)
        
        xy_plot(XYPoints(self.favs, self.nevents, style='bo'), 
                xlabel='Favorites', ylabel='Number of Events', 
                title='Favorites x Events (%.2f , %.2f)'%favs_events, 
                outputf=os.path.join(self.outf, 'total-favsxevts.png'), 
                logx=True, logy=False)
        
        write_xy_to_file(self.view, self.comm, 
                         os.path.join(self.outf, 'total-viewsxcomm.scat'))
        write_xy_to_file(self.view, self.favs, 
                         os.path.join(self.outf, 'total-viewsxfavs.scat'))
        write_xy_to_file(self.comm, self.favs, 
                         os.path.join(self.outf, 'total-commxfavs.scat'))
        
        write_xy_to_file(self.view, self.nevents, 
                         os.path.join(self.outf, 'total-viewxevts.scat'))
        write_xy_to_file(self.comm, self.nevents, 
                         os.path.join(self.outf, 'total-commxevts.scat'))
        write_xy_to_file(self.favs, self.nevents, 
                         os.path.join(self.outf, 'total-favsxevts.scat'))
        
class CorrelateTotal(YoutimeH5Runner):

    def __init__(self, name, description):
        super(CorrelateTotal, self).__init__(name, description)
        self.mapper_obj = None
        self.reducer_obj = None
    
    def item_generator(self):
        return self.igen
    
    def mapper(self):
        return self.mapper_obj
    
    def reducer(self):
        return self.reducer_obj
    
    def setup(self, arg_vals):
        super(CorrelateTotal, self).setup(arg_vals)
        self.mapper_obj = Noop()
        self.reducer_obj = CorrelateTotalReducer(arg_vals.outf)
        self.igen = PyTablesDaoIterator(arg_vals.in_file, arg_vals.table)
        
    def finalize(self):
        self.reducer_obj.close()
        
if __name__ == '__main__':
    runner = CorrelateTotal(sys.argv[0], __doc__)
    runner(sys.argv[1:])