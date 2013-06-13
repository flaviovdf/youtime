#-*- coding: utf8
'''
This module contains the code used to structure the parsed info and stats
files into dictionaries representing the data needed to process each video
'''
from __future__ import division, print_function

from scipy.interpolate import interp1d

from youtime.common import log
from youtime.common.constants import DAY

import numpy as np
from youtime.common.dao import VideoDAO

def _percentual_data_to_absolute(max_val, data):
    '''
    Given the maximum value and a series of percentages (0 to 100%) of this
    maximum, this method will convert the percentages to absolute values.
    
    Arguments
    ---------
    max_val: int
        the maximum value
    data: iterable of floats
        the percentages [0-100]%.
    '''
    
    return [round(x * max_val / 100) for x in data]

def _get_days_in_range(first_date, collect_date, num_points):
    '''
    When there are more than 100 points in each data array, we need to discover
    which date corresponds to every one of the 100 points. Given a collect date
    and the date of the first data point (which *must* be one day before the 
    video's upload date), this method will return the dates corresponding to
    the 100 points. If there are less than one hundred points, each point is 
    one day away from the previous. 
    '''
    #Range
    data_range = collect_date - first_date
    range_days = (int(data_range) // DAY) + 1
    
    #When we have less than 100 points, each point is already a day
    if num_points < 100 or range_days == 100:
        delta_between_points = DAY
    else: 
        #If not, points are evenly distributed in range. 
        #-1 because we have (num_points - 1) intervals between points
        delta_between_points = data_range / (num_points - 1)
    
    points = [first_date + i * delta_between_points for i in xrange(num_points)]     
    return range_days, points

def _to_delta(data):
    '''
    Given an array of contiguous values, computes the difference between each
    consecutive pairs of values.
    '''
    return [0] + [data[i] - data[i - 1] for i in xrange(1, len(data))]

def _fill(data_array, length):
    '''
    Returns a new array with the the given `length`. The array is structured as:
        [len(data_array), data_array[0], data_array[1], ..., data_array[n],
         0, 0, 0, ...]
    That is, the first element is the length of the given array (`data_array`),
    the return array is then filled with the values of the `data_array`. After
    that, the array contains zeros. This is needed because PyTables does not
    store variable sized arrays (lists).
    
    Arguments
    ---------
    data_array: numpy array
        array to create the fill
    length: int
        the required length
    '''
    return_val = np.zeros(length, dtype=data_array.dtype)
    return_val[0] = len(data_array)
    return_val[1:len(data_array) + 1] = data_array
    return return_val

def __real_create(video_id, range_days, upload_date, collect_date, first_date,
                  last_date, orig_view_data, orig_comm_data, orig_favs_data,
                  orig_dates, interp_view_data, interp_comm_data, 
                  interp_favs_data, interp_dates, events, honors_count):
    
    dao = {}
    dao[VideoDAO.ID] = video_id
    dao[VideoDAO.INT_ID] = hash(video_id)
    
    dao[VideoDAO.DAYS] = range_days
    dao[VideoDAO.UPLOAD_DATE] = upload_date
    dao[VideoDAO.COLLECT_DATE] = collect_date
    dao[VideoDAO.FIRST_DATE] = first_date
    dao[VideoDAO.LAST_DATE] = last_date
    
    dao[VideoDAO.TOTAL_VIEW] = orig_view_data.sum()
    dao[VideoDAO.TOTAL_COMM] = orig_comm_data.sum()
    dao[VideoDAO.TOTAL_FAVS] = orig_favs_data.sum()
    
    orig_points_fill = 101
    dao[VideoDAO.VIEW_DATA_ORIG] = _fill(orig_view_data, orig_points_fill)
    dao[VideoDAO.COMM_DATA_ORIG] = _fill(orig_comm_data, orig_points_fill)
    dao[VideoDAO.FAVS_DATA_ORIG] = _fill(orig_favs_data, orig_points_fill)
    dao[VideoDAO.DATE_POINTS_ORIG] = _fill(orig_dates, orig_points_fill)

    interp_points_fill = 2001
    dao[VideoDAO.VIEW_DATA_INTERP] = _fill(interp_view_data, interp_points_fill)
    dao[VideoDAO.COMM_DATA_INTERP] = _fill(interp_comm_data, interp_points_fill)
    dao[VideoDAO.FAVS_DATA_INTERP] = _fill(interp_favs_data, interp_points_fill)
    dao[VideoDAO.DATE_POINTS_INTERP] = _fill(interp_dates,  interp_points_fill)
        
    types = []
    dates = []
    views = []
    for event in events:
        name = event[0]
        date = event[1]
        view = event[2]
        
        types.append(name)
        dates.append(date)
        views.append(view)
    
    dao[VideoDAO.EVENT_TYPES] = _fill(np.array(types), 11)
    dao[VideoDAO.EVENT_DATES] = _fill(np.array(dates, dtype='int32'), 11)
    dao[VideoDAO.EVENT_VIEWS] = _fill(np.array(views, dtype='int32'), 11)
    dao[VideoDAO.HONORS] = honors_count
    
    return dao

def create(video_data, del_dates=None):
    
    #Extract variables from object
    video_id = video_data['VIDEO_ID']
    upload_date = video_data['UPLOAD_DATE']
    first_date = video_data['FIRST_DATE']
    collect_date = video_data['LAST_DATE']
    
    #Some videos only have data from long after the upload date. We want at most
    #two days difference. On most videos the first date is one day before the
    #upload date
    if ((first_date - upload_date) / DAY) > 0:
        log('first point is not on or one day before upload date %s' % video_id)
        return None
    
    #Converts to absolute values
    orig_view_data = _percentual_data_to_absolute(video_data['TOPY'], 
                                                  video_data['VIEW_DATA'])
    orig_comm_data = _percentual_data_to_absolute(video_data['TOTAL_COMM'], 
                                                  video_data['COMM_DATA'] )
    orig_favs_data = _percentual_data_to_absolute(video_data['TOTAL_FAVS'], 
                                                  video_data['FAVS_DATA'] )
    
    range_days, orig_dates = \
        _get_days_in_range(first_date, collect_date, len(orig_view_data))
    
    #Simple Interpolation
    interp_dates = []
    interp_view_data = []
    interp_comm_data = []
    interp_favs_data = []
    if len(orig_view_data) == 100:
        #One point for each day in range
        interp_dates = [first_date + (i * DAY) for i in xrange(range_days)]
        
        view_interp = interp1d(orig_dates, orig_view_data)
        comm_interp = interp1d(orig_dates, orig_comm_data)
        favs_interp = interp1d(orig_dates, orig_favs_data)
        
        for point in interp_dates:
            interp_view_data.append(round(view_interp(point)))
            interp_comm_data.append(round(comm_interp(point)))
            interp_favs_data.append(round(favs_interp(point)))
    else:
        interp_dates = orig_dates
        interp_view_data = orig_view_data
        interp_comm_data = orig_comm_data
        interp_favs_data = orig_favs_data
    
    #Convert to delta and numpy arrays
    orig_dates = np.array(orig_dates, dtype='int32')
    interp_dates = np.array(interp_dates, dtype='int32')
    orig_view_data = np.array(_to_delta(orig_view_data), dtype='int32')
    orig_comm_data = np.array(_to_delta(orig_comm_data), dtype='int32')
    orig_favs_data = np.array(_to_delta(orig_favs_data), dtype='int32')
    interp_view_data = np.array(_to_delta(interp_view_data), dtype='int32')
    interp_comm_data = np.array(_to_delta(interp_comm_data), dtype='int32')
    interp_favs_data = np.array(_to_delta(interp_favs_data), dtype='int32')
        
    #deletion dates is used for correction of YouTomb videos
    if del_dates:
        #Get valid date points 
        last_date = del_dates[video_id]
        before_deletion_orig = orig_dates <= last_date
        before_deletion_interp = interp_dates <= last_date
        
        #Filter invalid
        orig_dates = orig_dates[before_deletion_orig]
        interp_dates = interp_dates[before_deletion_interp]
        
        orig_view_data = orig_view_data[before_deletion_orig]
        orig_comm_data = orig_comm_data[before_deletion_orig]
        orig_favs_data = orig_favs_data[before_deletion_orig]
        
        interp_view_data = interp_view_data[before_deletion_interp]
        interp_comm_data = interp_comm_data[before_deletion_interp]
        interp_favs_data = interp_favs_data[before_deletion_interp]
    else:
        last_date = collect_date
    
    #Filter events after last date (some unknown data corruption)
    events = video_data['EVENTS']
    good_events = []
    for event_name, event_date, event_views in events:
        if event_date <= orig_dates[-1]:
            good_events.append((event_name, event_date, event_views))

    return __real_create(video_id, range_days, upload_date, collect_date, 
                         first_date, last_date, orig_view_data, 
                         orig_comm_data, orig_favs_data, orig_dates, 
                         interp_view_data, interp_comm_data, interp_favs_data, 
                         interp_dates, events, video_data['HONORS'])