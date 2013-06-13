# -*- coding: utf8
'''Common mappers shared throught the code'''

from __future__ import division, print_function

from vod.mapreducescript import BaseMapper

class Noop(BaseMapper):
    '''Noop mapper does nothing with items'''
    
    def _map(self, key, item):
        return item