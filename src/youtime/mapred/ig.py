# -*- coding: utf8
'''Common item generators used through out the code'''

from __future__ import division, print_function

from collections import defaultdict
from collections import Iterator
from itertools import chain
from tables import openFile

from youtime.common.dao import VideoDAO

import os

class ListDir(Iterator):
    '''
    This iterator returns the files which belong to a folder
    in a non-recursive manner. 
    ''' 
    
    #TODO: change ignore to be a regex
    def __init__(self, indir, ignore=None):
        self.indir = indir
        self.items = iter(os.listdir(self.indir))
        self.filter = ignore

    def next(self):
        item_name = self.items.next()
        if self.filter: 
            while self.filter in item_name: 
                item_name = self.items.next()
        
        full_path = os.path.join(self.indir, item_name)
        return (item_name, full_path)

class PyTablesDaoIterator(Iterator):
    '''
    A generator over VideoDAOs stored on an H5 PyTables file.
    ''' 
    
    def __init__(self, pytfpath, tname=None):
        
        self.pytfpah = pytfpath
        self.tname = tname
        self.file = openFile(self.pytfpah, 'r')
        
        table = None
        if self.tname is None or self.tname == 'all':
            table = chain(self.file.root.days,
                               self.file.root.weeks,
                               self.file.root.months,
                               self.file.root.years)
            
        elif self.tname == 'days':
            table = iter(self.file.root.days)
        elif self.tname == 'weeks':
            table = iter(self.file.root.weeks)
        elif self.tname == 'months':
            table = iter(self.file.root.months)
        elif self.tname == 'years':
            table = iter(self.file.root.years)
        else:
            raise Exception()
        
        self.table = table
    
    def next(self):
        video_row = self.table.next()
        dao = VideoDAO(video_row)
        return dao[VideoDAO.ID], dao

class DuplicateGroupsIgen(object):
    '''
    Given a h5 PyTables VideoDAO file and a file with a list of duplicate videos
    (one group of duplicates per line), this generator will yield the referrer
    information for each group.
    '''
    
    def __init__(self, pytfpath, duplicates_fpath, tname=None, min_vids=1):
        self.pytables_gen = PyTablesDaoIterator(pytfpath, tname)
        self.duplicates_fpath = duplicates_fpath
        self.min_vids = min_vids
        
    def __iter__(self):
        '''
        This method will create a set of sets of video events
        for each duplicate group.
        '''
        groups = VideoDAO.EV2GROUP.copy()
        
        #Grouping events and views. 
        id2event_data = {}
        id2total_views = {}
        for video_id, dao in self.pytables_gen:
            event_types = dao[VideoDAO.EVENT_TYPES]
            event_views = dao[VideoDAO.EVENT_VIEWS]
            
            #Summing up group views
            grouped_sum = defaultdict(int)
            for i, event_type in enumerate(event_types):
                event_group = groups[event_type]
                grouped_sum[event_group] += event_views[i]
            
            if len(grouped_sum) > 0:
                id2event_data[video_id] = frozenset(grouped_sum.iteritems())
                id2total_views[video_id] = dao[VideoDAO.TOTAL_VIEW]
            
        #Yields groups
        with open(self.duplicates_fpath) as duplicates_file:
            for line in duplicates_file:
                spl = line.split()
                to_yield = {}
                for duplicate_id in spl:
                    if duplicate_id in id2total_views:
                        total_view = id2total_views[duplicate_id]
                        to_yield[duplicate_id] = (id2event_data[duplicate_id], 
                                                  total_view)
                        
                if len(to_yield) > self.min_vids:
                    #We are ignoring keys.
                    yield None, to_yield 