# -*- coding: utf-8
'''
Map reduce script to parse HTML files and extract the video uploader,
category and tags for each video.
'''
from __future__ import division, print_function

from vod.mapreducescript import BaseMapper
from vod.mapreducescript import BaseReducer
from vod.mapreducescript import Runner

from youtime.parser.info_files import category_tags_user_en 
from youtime.mapred.ig import ListDir

import io
import sys

class UCTMapper(BaseMapper):
    
    def _map(self, key, item):
        return category_tags_user_en(item)

class UCTReducer(BaseReducer):
    
    def __init__(self, out_fpath):
        super(UCTReducer, self).__init__()
        self.out_file = io.open(out_fpath, 'w')
    
    def _reduce(self, key, value):
        if value and len(value) > 0:
            for videoid, txt in value.items():
                self.out_file.write(u'%s %s\n' % (videoid, txt))
    
    def close(self):
        self.out_file.close()

class UCT(Runner):

    def __init__(self, name, description):
        super(UCT, self).__init__(name, description)
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
        self.mapper_obj = UCTMapper()
        self.reducer_obj = UCTReducer(arg_vals.outf)
        
if __name__ == '__main__':
    runner = UCT(sys.argv[0], __doc__)
    runner(sys.argv[1:])