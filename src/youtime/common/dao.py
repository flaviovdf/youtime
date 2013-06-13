# -*- coding: utf8
'''
Module to create abstractions from PyTables to objects to be processed
'''
from __future__ import division, print_function

from collections import Hashable

from tables import *

def _nop(field):
    '''
    This method simple returns the field. It is a hack because
    pytables rows cannot be serialized.
    '''
    return field

def _pc(field):
    '''
    Method for converting arrays. Since PyTables can only store fixed size
    arrays, we make them large enough for every video, so we have to "trim"
    arrays. The way we do this is make the first element of the array to
    be the size of valid data points.
    '''
    return field[1 : int(float(field[0])) + 1]

class VideoDAO(Hashable):
    '''
    This class represents a dict like interface for a video to be processed.
    The name of the fields which can be accessed are the constants of the
    class.
    '''

    ID = 'ID'
    INT_ID = 'INT_ID'

    EV_GROUPS = {
        'VIRAL':set(['Other / Viral']),
        'SOCIAL':set([
                      'First referral from a subscriber', 
                      'First view on a channel page'              
                      ]),
        'SEARCH':set([
                      'First referral from Google searc',
                      'First referral from YouTube sear',
                      'First referral from Google Video' 
                     ]),
        'EXTERNAL':set([
                        'First embedded view',
                        'First embedded on',
                        'First referral from'
                     ]),
        'INTERNAL':set([
                        'First referral from YouTube',
                        'First referral from related vide'
                        ]),
        'FEATURED':set([
                        'First view from ad',
                        'First featured video view'
                     ]),
        'MOBILE':set(['First view from a mobile device']),
        'NOT_CAPTURED':set(['NOT_CAPTURED'])
    }

    #Inverts the EV_GROUPS dict
    EV2GROUP = dict([(i, k) for k, v in EV_GROUPS.items() for i in v])

    #Group to style. Useful for plotting and maintaining same style
    GROUP2STYLE = {
        'VIRAL':'r-',
        'SOCIAL':'y--',
        'SEARCH':'b-.',
        'EXTERNAL':'g-',
        'INTERNAL':'k--',
        'FEATURED':'m-.',
        'MOBILE':'c-',
        'NOT_CAPTURED':'ko-'
    }

    DAYS      = 'DAYS'
    
    UPLOAD_DATE = 'UPLOAD_DATE'
    COLLECT_DATE = 'COLLECT_DATE'
    FIRST_DATE = 'FIRST_DATE'
    LAST_DATE = 'LAST_DATE'
    
    TOTAL_VIEW = 'TOTAL_VIEW'
    TOTAL_COMM = 'TOTAL_COMM'
    TOTAL_FAVS = 'TOTAL_FAVS'
    
    VIEW_DATA_ORIG = 'VIEW_DATA_ORIG'
    COMM_DATA_ORIG = 'COMM_DATA_ORIG'
    FAVS_DATA_ORIG = 'FAVS_DATA_ORIG'
    DATE_POINTS_ORIG = 'DATE_POINTS_ORIG'

    VIEW_DATA_INTERP = 'VIEW_DATA_INTERP'
    COMM_DATA_INTERP = 'COMM_DATA_INTERP'
    FAVS_DATA_INTERP = 'FAVS_DATA_INTERP'
    DATE_POINTS_INTERP = 'DATE_POINTS_INTERP'
    
    EVENT_TYPES = 'EVENT_TYPES'
    EVENT_DATES = 'EVENT_DATES'
    EVENT_VIEWS = 'EVENT_VIEWS'
    
    HONORS = 'HONORS'

    def __init__(self, raw_data):
        converters = self.get_converters()
        self.converted = {}
        
        self.converted[self.__class__.INT_ID] = \
            int(raw_data[self.__class__.INT_ID])
        self.converted[self.__class__.ID] = \
            raw_data[self.__class__.ID]
            
        for key in self.get_converters():
            conv = converters[key]
            self.converted[key] = conv(raw_data[key])

    def __getitem__(self, i):
        return self.converted[i]

    def __hash__(self):
        return self[self.__class__.INT_ID]

    def get_converters(self):
        '''
        Returns Callables which treat array like fields. 
        These fields have the length as the first element,
        so we remove this and trim anything beyond length. 
        '''
        #Arrays must be treated
        return {
                self.__class__.ID:_nop,
                self.__class__.INT_ID:_nop,
                
                self.__class__.DAYS:_nop,
                self.__class__.FIRST_DATE:_nop,
                self.__class__.LAST_DATE:_nop,
                self.__class__.COLLECT_DATE:_nop,
                self.__class__.UPLOAD_DATE:_nop,
                self.__class__.HONORS:_nop,
                
                self.__class__.TOTAL_VIEW:_nop,
                self.__class__.TOTAL_COMM:_nop,
                self.__class__.TOTAL_FAVS:_nop,
    
                self.__class__.VIEW_DATA_ORIG:_pc,
                self.__class__.COMM_DATA_ORIG:_pc,
                self.__class__.FAVS_DATA_ORIG:_pc,
                self.__class__.DATE_POINTS_ORIG:_pc,

                self.__class__.VIEW_DATA_INTERP:_pc,
                self.__class__.COMM_DATA_INTERP:_pc,
                self.__class__.FAVS_DATA_INTERP:_pc,
                self.__class__.DATE_POINTS_INTERP:_pc,
                
                self.__class__.EVENT_TYPES:_pc,
                self.__class__.EVENT_DATES:_pc,
                self.__class__.EVENT_VIEWS:_pc
            }
        
class VideoDesc(IsDescription):
    INT_ID    = Int64Col(pos=1) #@UndefinedVariable
    ID        = StringCol(128, pos=2) #@UndefinedVariable

    DAYS      = Int32Col() #@UndefinedVariable
    UPLOAD_DATE = Time32Col() #@UndefinedVariable
    COLLECT_DATE = Time32Col() #@UndefinedVariable
    FIRST_DATE  = Time32Col() #@UndefinedVariable
    LAST_DATE   = Time32Col() #@UndefinedVariable
    
    TOTAL_VIEW = Int32Col() #@UndefinedVariable
    TOTAL_COMM = Int32Col() #@UndefinedVariable
    TOTAL_FAVS = Int32Col() #@UndefinedVariable

    VIEW_DATA_ORIG = Int32Col(shape=(101,)) #@UndefinedVariable
    COMM_DATA_ORIG = Int32Col(shape=(101,)) #@UndefinedVariable
    FAVS_DATA_ORIG = Int32Col(shape=(101,)) #@UndefinedVariable
    DATE_POINTS_ORIG = Int32Col(shape=(101,)) #@UndefinedVariable

    VIEW_DATA_INTERP = Int32Col(shape=(2001,)) #@UndefinedVariable
    COMM_DATA_INTERP = Int32Col(shape=(2001,)) #@UndefinedVariable
    FAVS_DATA_INTERP = Int32Col(shape=(2001,)) #@UndefinedVariable
    DATE_POINTS_INTERP = Int32Col(shape=(2001,)) #@UndefinedVariable
    
    EVENT_TYPES = StringCol(32, shape=(11,)) #@UndefinedVariable
    EVENT_DATES = Time32Col(shape=(11,)) #@UndefinedVariable
    EVENT_VIEWS = Int32Col(shape=(11,)) #@UndefinedVariable
    
    HONORS = Int32Col() #@UndefinedVariable
