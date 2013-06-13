# -*- coding: utf-8
from __future__ import division, print_function

from itertools import izip

from vod.mapreducescript import BaseMapper
from vod.mapreducescript import BaseReducer

from youtime import YoutimeH5Runner
from youtime.common.constants import DAY
from youtime.common.constants import TSERIES_SIZE 
from youtime.common.dao import VideoDAO
from youtime.mapred.ig import PyTablesDaoIterator

import numpy as np

import csv
import os
import sys

ID = '#ID'

GROWTH_COMM = 'Z_GROWTH_COMM'
GROWTH_FAVS = 'Z_GROWTH_FAVS'
GROWTH_VIEWS = 'Z_GROWTH_VIEWS'

SUM_COMM = 'Z_SUM_COMM'
SUM_FAVS = 'Z_SUM_FAVS'
SUM_VIEWS = 'Z_SUM_VIEWS'
UPLOAD_DATE = 'A_UPLOAD_DATE'

date_label = lambda event_group: 'X_' + event_group + '_EVENT_DATE'
view_label = lambda event_group: 'Y_' + event_group + '_TOTAL_VIEWS'

__field_names = None
def get_field_names():
    global __field_names
    if not __field_names:
        groups = VideoDAO.EV_GROUPS.copy()
        del groups['NOT_CAPTURED']
        
        __field_names = [ID, UPLOAD_DATE, SUM_VIEWS, GROWTH_COMM, 
                         GROWTH_FAVS, GROWTH_VIEWS, SUM_COMM, SUM_FAVS]
        for g in groups:
            __field_names.append(view_label(g))
            __field_names.append(date_label(g))
        
        __field_names = sorted(__field_names)
    
    return __field_names

def get_avg_rate(x):
    if x.sum() != 0:
        return np.diff(x).mean()
    else:
        return 0

class EventsTableMapper(BaseMapper):

    def _map(self, key, item):
        
        if len(item[VideoDAO.VIEW_DATA_ORIG]) < TSERIES_SIZE:
            return None
        
        video_views = item[VideoDAO.VIEW_DATA_INTERP]
        video_comments = item[VideoDAO.COMM_DATA_INTERP]
        video_favorites = item[VideoDAO.FAVS_DATA_INTERP]
        
        video_upload_date = item[VideoDAO.FIRST_DATE]
        video_collect_date = item[VideoDAO.LAST_DATE]
        delta = DAY
        
        ev_types = item[VideoDAO.EVENT_TYPES]
        ev_dates = item[VideoDAO.EVENT_DATES]
        ev_views = item[VideoDAO.EVENT_VIEWS]

        ev2group = VideoDAO.EV2GROUP
        groups = VideoDAO.EV_GROUPS.copy()
        del groups['NOT_CAPTURED']
        
        return_val = {}
        windows = range(1, TSERIES_SIZE + 1) + [-1]
        for time_window in windows:
            
            print_ev_views = time_window == -1
            views_so_far = video_views[:time_window]
            comms_so_far = video_comments[:time_window]
            favs_so_far = video_favorites[:time_window]
            
            current_row = {}
            return_val[time_window] = current_row
            
            current_row[ID] = key
            current_row[UPLOAD_DATE] = video_upload_date
            
            for ev_group in groups:
                current_row[date_label(ev_group)] = 0
                current_row[view_label(ev_group)] = 0
            
            sum_views = views_so_far.sum()
            peak_view = views_so_far.max()
            peak_view_frac = peak_view / sum_views if sum_views > 0 else 0
            
            peak_time = video_upload_date + views_so_far.argmax() * delta 
            
            current_row[SUM_VIEWS] = sum_views
            current_row[SUM_COMM] = favs_so_far.sum()
            current_row[SUM_FAVS] = comms_so_far.sum()

            current_row[GROWTH_VIEWS] = get_avg_rate(views_so_far)
            current_row[GROWTH_COMM] = get_avg_rate(comms_so_far)
            current_row[GROWTH_FAVS] = get_avg_rate(favs_so_far)
            
            grouped_dates = {}
            grouped_views = {}
            for ev_type, ev_date, ev_view in izip(ev_types, ev_dates, ev_views):
                ev_group = ev2group[ev_type]
                    
                if ev_group not in grouped_dates:
                    grouped_views[ev_group] = ev_view
                    grouped_dates[ev_group] = ev_date
                else:
                    prev_date = grouped_dates[ev_group]
                    grouped_dates[ev_group] = min(prev_date, ev_date)
                    grouped_views[ev_group] = grouped_views[ev_group]+ ev_view
            
            date_so_far = video_upload_date + (time_window + 1) * delta
            for ev_group, group_date in grouped_dates.items():
                if group_date <= date_so_far:
                    current_row[date_label(ev_group)] = video_upload_date
                
                if print_ev_views:
                    current_row[view_label(ev_group)] = grouped_views[ev_group] 
                     
        return return_val
        
class EventsTableReducer(BaseReducer):

    def __init__(self, output_fold_path):
        self.output_fold_path = output_fold_path
        self.open_writers = {}
        self.open_files = {}
        self.fields = get_field_names()
    
    def _get_or_create_writer(self, writer_label):
        out_writer = None
        if writer_label in self.open_writers:
            out_writer = self.open_writers[writer_label]
        else:
            out_fpath = os.path.join(self.output_fold_path, 
                                'ev-%s-pts-nointerp.dat' % str(writer_label))
            out_file = open(out_fpath, 'w')
            
            print(' '.join(self.fields), file=out_file)
            out_writer = csv.DictWriter(out_file, self.fields, delimiter=' ')
            
            self.open_files[writer_label] = out_file
            self.open_writers[writer_label] = out_writer
            
        return out_writer
    
    def _reduce(self, key, value):
        if value:
            windows = value.keys()
            for time_window in windows:
                out_writer = self._get_or_create_writer(time_window)
                out_writer.writerow(value[time_window])
                
                if time_window == -2:
                    all_writer = self._get_or_create_writer('all')
                    all_writer.writerow(value[time_window])
        
    def close(self):
        for label in self.open_files:
            self.open_files[label].close()
    
class EventsTableRunner(YoutimeH5Runner):

    def __init__(self, name, description):
        super(EventsTableRunner, self).__init__(name, description)
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
        self.mapper_obj = EventsTableMapper()
        self.reducer_obj = EventsTableReducer(arg_vals.outf)
    
    def finalize(self):
        self.reducer_obj.close()
    
if __name__ == '__main__':
    runner = EventsTableRunner(sys.argv[0], __doc__)
    runner(sys.argv[1:])
