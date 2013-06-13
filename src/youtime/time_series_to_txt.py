# -*- coding: utf-8
'''
Converts interpolated time series to a text matrix with each video's time series
as a row.
'''
from __future__ import division, print_function

from vod.mapreducescript import BaseMapper
from vod.mapreducescript import BaseReducer

from youtime import YoutimeH5Runner
from youtime.common.constants import TSERIES_SIZE
from youtime.common.dao import VideoDAO
from youtime.mapred.ig import PyTablesDaoIterator

import sys
import os

class TimeSeriesMapper(BaseMapper):

    def __init__(self, interpolate):
        self.interpolate = interpolate

    def _map(self, key, item):
        view_orig = item[VideoDAO.VIEW_DATA_ORIG]
        view_interp = item[VideoDAO.VIEW_DATA_INTERP]
        
        date_orig = item[VideoDAO.DATE_POINTS_ORIG]
        date_interp = item[VideoDAO.DATE_POINTS_INTERP]
        
        if len(view_orig) >= TSERIES_SIZE:
            if self.interpolate:
                return view_interp, date_interp
            else:
                return view_orig, date_orig
        else:
            return None
                
class TimeSeriesReducer(BaseReducer):
    
    def __init__(self, output_folder, interpolate):
        self.output_folder = output_folder
        
        if interpolate:
            append = 'interpolated'
        else:
            append = 'nointerp'
            
        series_name = 'series_%s_%dpts.txt' %(append, TSERIES_SIZE)
        dates_name = 'dates_%s_%dpts.txt' %(append, TSERIES_SIZE)
        
        series_fpath = os.path.join(output_folder, series_name)
        self.series_file = open(series_fpath, 'w')

        dates_fpath = os.path.join(output_folder, dates_name)
        self.dates_file = open(dates_fpath, 'w')

    def _reduce(self, key, value):
        if value is not None:
            tseries, dates = value
            
            print(key, file=self.series_file, end=' ')
            for view in tseries:
                print(view, file=self.series_file, end=' ')
            print(file=self.series_file)

            print(key, file=self.dates_file, end=' ')
            for date in dates:
                print(date, file=self.dates_file, end=' ')
            print(file=self.dates_file)

    def close(self):
        self.dates_file.close()
        self.series_file.close()
    
class TimeSeriesRunner(YoutimeH5Runner):
    
    def __init__(self, name, description):
        super(TimeSeriesRunner, self).__init__(name, description)
        self.igen = None
        self.mapper_obj = None
        self.reducer_obj = None
    
    def item_generator(self):
        return self.igen
    
    def mapper(self):
        return self.mapper_obj
    
    def reducer(self):
        return self.reducer_obj
    
    def setup(self, arg_vals):
        self.mapper_obj = TimeSeriesMapper(arg_vals.interpolate)
        self.reducer_obj = TimeSeriesReducer(arg_vals.outf, 
                                             arg_vals.interpolate)
        self.igen = PyTablesDaoIterator(arg_vals.in_file, arg_vals.table)
    
    def add_custom_aguments(self, parser):
        super(TimeSeriesRunner, self).add_custom_aguments(parser)
        parser.add_argument('--interpolate', action='store_true',
                            help='Interpolate curve?')
    
    def finalize(self):
        self.reducer_obj.close()
        
if __name__ == '__main__':
    runner = TimeSeriesRunner(sys.argv[0], __doc__)
    runner(sys.argv[1:])