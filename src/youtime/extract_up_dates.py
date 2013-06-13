# -*- coding: utf-8
'''
Map reduce script to parse HTML files and extract video upload dates
'''
from __future__ import division, print_function

from vod.fileutil import write_xy_to_file
from vod.mapreducescript import BaseMapper
from vod.mapreducescript import BaseReducer
from vod.mapreducescript import Runner

from youtime.parser.info_files import up_dates_html_ptbr 
from youtime.mapred.ig import ListDir

import os
import sys

class UpDatesMapper(BaseMapper):
    
    def _map(self, key, item):
        dates = up_dates_html_ptbr(item)
        return dates

class UpDatesReducer(BaseReducer):
    
    def __init__(self, out_file):
        super(UpDatesReducer, self).__init__()
        assert (not os.path.exists(out_file))
        self.out_file = out_file
        self.dates = {}
    
    def _reduce(self, key, value):
        for videoid, date in value.items():
            self.dates[videoid] = date
    
    def close(self):
        xvals = []
        yvals = []
        for videoid, date in self.dates.items():
            xvals.append(videoid)
            yvals.append(date)
        write_xy_to_file(xvals, yvals, self.out_file)

class UpDates(Runner):

    def __init__(self, name, description):
        super(UpDates, self).__init__(name, description)
        self.igen_obj = None
        self.mapper_obj = None
        self.reducer_obj = None
    
    def item_generator(self):
        return self.igen_obj
    
    def mapper(self):
        return self.mapper_obj
    
    def reducer(self):
        return self.reducer_obj
    
    def finalize(self):
        self.reducer_obj.close()
        
    def add_custom_aguments(self, parser):
        parser.add_argument('indir', type=str, 
                            help='In directory')
        
        parser.add_argument('outf', type=str, 
                            help='Output file')
        
    def setup(self, arg_vals):
        self.igen_obj = ListDir(arg_vals.indir, ignore='stat')
        self.mapper_obj = UpDatesMapper()
        self.reducer_obj = UpDatesReducer(arg_vals.outf)
        
if __name__ == '__main__':
    runner = UpDates(sys.argv[0], __doc__)
    runner(sys.argv[1:])