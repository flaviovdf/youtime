# -*- coding: utf-8
'''
This script plots the CDF of the fraction of views in the three most popular
days in the timeseries of videos.
'''
from __future__ import division, print_function

from heapq import nlargest

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

class PeakFinderMapper(BaseMapper):
    
    def __init__(self, cname):
        if cname == 'view':
            self.dname = VideoDAO.VIEW_DATA_INTERP
            self.tname = VideoDAO.TOTAL_VIEW
        elif cname == 'comm':
            self.dname = VideoDAO.COMM_DATA_INTERP
            self.tname = VideoDAO.TOTAL_COMM
        elif cname == 'favs':
            self.dname = VideoDAO.FAVS_DATA_INTERP
            self.tname = VideoDAO.TOTAL_FAVS    
        else:
            raise Exception()
        
    def _map(self, key, item):
        statistics = item[self.dname]
        total = item[self.tname]
        
        if total == 0.0:
            return None
        
        days = 0
        weeks = 0
        months = 0
        
        data_day = {}
        data_week = {}
        data_month = {}
        
        sum_week = 0.0
        sum_month = 0.0
        
        for i in xrange(len(statistics)):
            stat = statistics[i]
            days = i
            
            sum_week += stat
            sum_month += stat
            
            data_day[days] = stat
            if days % 7 == 0:
                data_week[weeks] = sum_week
                weeks += 1
                sum_week = 0.0
            if days % 30 == 0:
                data_month[months] = sum_month
                months += 1
                sum_month = 0.0
        
        daily_top = nlargest(3, data_day.values())
        weekly_top = nlargest(3, data_week.values())
        monthly_top = nlargest(3, data_month.values())
        
        daily_frac = None
        if len(daily_top) >= 3:
            daily_frac = (daily_top[0] / total, 
                          daily_top[1] / total, 
                          daily_top[2] / total)
        
        weekly_frac = None    
        if len(weekly_top) >= 3:
            weekly_frac = (weekly_top[0] / total, 
                           weekly_top[1] / total,
                           weekly_top[2] / total)
        
        monthly_frac = None    
        if len(monthly_top) >= 3:
            monthly_frac = (monthly_top[0] / total, 
                            monthly_top[1] / total, 
                            monthly_top[2] / total)
        
        return (daily_frac, weekly_frac, monthly_frac)

class PeakFinderReducer(BaseReducer):
        
    def __init__(self, outf, cname):
        assert(os.path.isdir(outf))
        self.outf = outf
        
        self.day_data = []
        self.week_data = []
        self.month_data = []
        
        if cname == 'view':
            self.gname = 'views'
        elif cname == 'comm':
            self.gname = 'comments'
        elif cname == 'favs':
            self.gname = 'favorites'    
        elif cname == 'avgr':
            self.gname = 'average rating'
        elif cname == 'rats':
            self.gname = 'number of ratings'
        else:
            raise Exception()
        
        self.cname = cname
        
    def _reduce(self, key, value):
        if value:
            if value[0]:
                self.day_data.append(value[0])
            if value[1]:
                self.week_data.append(value[1])
            if value[2]:
                self.month_data.append(value[2])
    
    def close(self):
        self.real_close(self.day_data, 'day')
        self.real_close(self.week_data, 'week')
    
    def real_close(self, tdata, append):
        frac_tf = []
        frac_ts = []
        frac_tt = []
        
        for tf, ts, tt in tdata:
            frac_tf.append(tf)
            frac_ts.append(ts)
            frac_tt.append(tt)
        
        tf_cdf = ecdf(frac_tf)
        ts_cdf = ecdf(frac_ts)
        tt_cdf = ecdf(frac_tt)
        
        line_tf = XYPoints(tf_cdf[0], tf_cdf[1],
                           r'Peak '+append, 'reducer-')
        line_ts = XYPoints(ts_cdf[0], ts_cdf[1],
                           r'$2^{nd}$ peak '+append, 'k--')
        line_tt = XYPoints(tt_cdf[0], tt_cdf[1],
                           r'$3^{rd}$ peak '+append, 'k:')
        
        xy_plot(line_tt, line_ts, line_tf, 
                 xlabel=r'Fraction of '+self.gname+' on peak '+append+' - $f$', 
                 ylabel=r'Prob. (Fraction of '+self.gname+' $\leq$ f)', 
                 legloc = 'lower right', 
                 legborder = False,
                 xmin=0.0, xmax=1.0,
                 ymin=0.0, ymax=1.0,
                 outputf=os.path.join(self.outf, 
                                        self.cname+'-'+append+'-cdf.png'))

        write_stats_to_file(frac_tf, 
                            os.path.join(self.outf, 
                                         self.cname+'-'+append+'-first.stats'))
        write_stats_to_file(frac_ts, 
                            os.path.join(self.outf, 
                                         self.cname+'-'+append+'-second.stats'))
        write_stats_to_file(frac_tt, 
                            os.path.join(self.outf, 
                                         self.cname+'-'+append+'-third.stats'))
        
        write_xy_to_file(tf_cdf[0], tf_cdf[1], 
                         os.path.join(self.outf,
                                      self.cname+'-'+append+'-first.cdf'))
        write_xy_to_file(ts_cdf[0], ts_cdf[1], 
                         os.path.join(self.outf,
                                      self.cname+'-'+append+'-second.cdf'))
        write_xy_to_file(tt_cdf[0], tt_cdf[1], 
                         os.path.join(self.outf, 
                                      self.cname+'-'+append+'-third.cdf'))
        
class PeakFinder(YoutimeH5Runner):
    
    def __init__(self, name, description):
        super(PeakFinder, self).__init__(name, description)
        self.mapper_obj = None
        self.reducer_obj = None
    
    def item_generator(self):
        return self.igen
    
    def mapper(self):
        return self.mapper_obj
    
    def reducer(self):
        return self.reducer_obj
    
    def add_custom_aguments(self, parser):
        super(PeakFinder, self).add_custom_aguments(parser)
        parser.add_argument(dest='cname', type = str,
                            help='Column Name')
        
    def setup(self, arg_vals):
        self.igen = PyTablesDaoIterator(arg_vals.in_file, arg_vals.table)
        self.mapper_obj = PeakFinderMapper(arg_vals.cname)
        self.reducer_obj = PeakFinderReducer(arg_vals.outf, arg_vals.cname)
    
    def finalize(self):
        self.reducer_obj.close()
        
if __name__ == '__main__':
    runner = PeakFinder(sys.argv[0], __doc__)
    runner(sys.argv[1:])