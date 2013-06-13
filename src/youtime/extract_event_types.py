# -*- coding: utf-8
'''
Map reduce script which prints out every event type
found in raw html files. 
'''
from __future__ import division, print_function

from vod.mapreducescript import BaseMapper
from vod.mapreducescript import Runner

from youtime.mapred.ig import ListDir

import re
import sys

LINKS_START_RE = r'Links\s+'
PRE_EVENT_TYPE_RE = r'\s+<td>\s+$'

class ExtractEventsMapper(BaseMapper):
    
    def __init__(self):
        self.links_start_matcher = re.compile(LINKS_START_RE)
        self.ev_type_matcher =  re.compile(PRE_EVENT_TYPE_RE)
        
    def _map(self, key, item):
        return_val = []
        
        links = False
        with open(item) as html_file:
            for line in html_file:
                match = self.links_start_matcher.match(line)
                if match:
                    links = True
                    continue
                
                if links:
                    match = self.ev_type_matcher.match(line)
                    if match:
                        next_line = html_file.next()
                        return_val.append(next_line.strip())
        
        return return_val

def _reduce(key, value):
    '''
    This reducer simply prints out values.
    '''
    if value:
        for event_type in value:
            print(event_type)

class ExtractEventsRunner(Runner):

    def __init__(self, name, description):
        super(ExtractEventsRunner, self).__init__()
        self.igen = None
        self.mapper_obj = None
        self.reducer_obj = None
    
    def item_generator(self):
        return self.igen
    
    def mapper(self):
        return self.mapper_obj
    
    def reducer(self):
        return self.reducer_obj
    
    def add_custom_aguments(self, parser):
        parser.add_argument('indir',  type=str, 
                          help='In directory')

    
    def setup(self, arg_vals):
        self.igen = ListDir(arg_vals.indir, filter='stat')
        self.mapper_obj = ExtractEventsMapper()
        self.reducer_obj = _reduce
        
if __name__ == '__main__':
    runner = ExtractEventsRunner(sys.argv[0], __doc__)
    runner(sys.argv[1:])