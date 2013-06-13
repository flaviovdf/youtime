# -*- coding: utf-8
'''
Map reduce scripts used for data analysis are kept here. This
module also contains the common runner, `YoutimeH5Runner` for
most scripts.
'''
from __future__ import division, print_function

from collections import Iterator

from vod.mapreducescript import Runner

from youtime.common.dao import VideoDAO
from youtime.mapred.ig import PyTablesDaoIterator

import abc

class YoutimeH5Runner(Runner):
    '''
    This class has the basic options for most map reduce scripts used to
    process videos.
    '''
    
    __metaclass__ = abc.ABCMeta

    def __init__(self, name, description):
        super(YoutimeH5Runner, self).__init__(name, description)
        self.igen = None

    def add_custom_aguments(self, parser):
        parser.add_argument('in_file',  type=str, 
                            help='In file (H5 PyTables)')
        parser.add_argument('table', type=str, 
                            help='Table name')
        parser.add_argument('outf',  type=str, 
                            help='Output folder file')