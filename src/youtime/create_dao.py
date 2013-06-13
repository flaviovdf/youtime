# -*- coding: utf-8
'''
Map reduce script to parse HTML files and generate VideoDAO
objects in a H5 database.
'''
from __future__ import division, print_function

from vod.mapreducescript import BaseMapper
from vod.mapreducescript import BaseReducer
from vod.mapreducescript import Runner

from youtime.common import log
from youtime.common.dao import VideoDesc
from youtime.parser.dao_creator import create
from youtime.mapred.ig import ListDir
from youtime.parser import stats_files 

from tables import openFile

import sys

class CreateDAOMapper(BaseMapper):
    
    def __init__(self, up_dates, del_dates):
        self.up_dates = up_dates 
        self.del_dates = del_dates
    
    def _map(self, key, item):
        videos = stats_files.parse_stats(item, self.up_dates)
        daos = []
        for vid in videos:
            try:
                dao = create(vid, self.del_dates)
                if dao != None:
                    daos.append(dao)
            except:
                log('Unable to create video')
                
        return daos

class DaoReducer(BaseReducer):
    
    def __init__(self, out_file):
        super(DaoReducer, self).__init__()
        
        self.out_file = out_file
        
        self.h5f = openFile(self.out_file, 'w')
        self.days   = self.h5f.createTable(self.h5f.root, 
                                           'days', VideoDesc)
        self.weeks  = self.h5f.createTable(self.h5f.root, 
                                           'weeks', VideoDesc)
        self.months = self.h5f.createTable(self.h5f.root, 
                                           'months', VideoDesc)
        self.years  = self.h5f.createTable(self.h5f.root, 
                                           'years', VideoDesc)
    
    def _reduce(self, key, value):
        if value:
            for dao in value:
                days = dao['DAYS']
                
                table = None
                if days <= 7:
                    table = self.days
                elif days <= 30:
                    table = self.weeks
                elif days <= 365:
                    table = self.months
                else:
                    table = self.years
                row = table.row
            
                for key, val in dao.items():
                    row[key] = val
                row.append()
    
    def close(self):
        self.days.flush()
        self.weeks.flush()
        self.months.flush()
        self.years.flush()
        
        self.h5f.flush()
        self.h5f.close()

class CreateDAO(Runner):

    def __init__(self, name, description):
        super(CreateDAO, self).__init__(name, description)
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
        
        parser.add_argument('dates', type=str, 
                          help='File with video id, upload and deletion date')
        
        parser.add_argument('--del_dates',  action='store_true', 
                            help='Indicates if deletion dates are on the file')
        
    def setup(self, arg_vals):
        up_dates_dict = {}
        del_dates_dict = None
        
        if arg_vals.del_dates:
            del_dates_dict = {}
            
        with open(arg_vals.dates) as dates_file:
            for line in dates_file:
                spl = line.split()
                id_ = spl[0]
                up_date = float(spl[1])
                up_dates_dict[id_] = up_date
                
                if del_dates_dict:
                    del_date = float(spl[2])
                    del_dates_dict[id_] = del_date
        
        self.igen_obj = ListDir(arg_vals.indir, ignore='info')
        self.mapper_obj = CreateDAOMapper(up_dates_dict, del_dates_dict)
        self.reducer_obj = DaoReducer(arg_vals.outf)
        
if __name__ == '__main__':
    runner = CreateDAO(sys.argv[0], __doc__)
    runner(sys.argv[1:])