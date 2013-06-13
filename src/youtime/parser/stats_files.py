# -*- coding: utf8
'''
This module contains the functions used for parsing stats files (those with time 
series and events). The base method to be used is `parse_stats`.
'''
from __future__ import division, print_function

from BeautifulSoup import BeautifulSoup
from BeautifulSoup import BeautifulStoneSoup

from time import mktime
from time import strptime

from youtime.parser import END_OF_VID_RE
from youtime.parser import VIDEOID_RE

import re

#Compiles regex here, saves time.
VID_BEGIN_MATCHER = re.compile(VIDEOID_RE)
VID_END_MATCHER = re.compile(END_OF_VID_RE)

VIEW_URL_RE = r'.*?chxl=1:(.*?)&.*?chxp=1,(.*?)&.*?chxr=(.*?)&.*?chd=t:(.*?)&.*'
VIEW_URL_MATCHER = re.compile(VIEW_URL_RE)

DATA_URL_RE = r'.*?chd=t:(.*)'
DATA_URL_MATCHER = re.compile(DATA_URL_RE)

TO_INT = lambda string: int(string.strip().replace(',',''))

def _get_video_xmls(fpath, up_dates):
    '''
    This method filters out only the xml content of the stats file
    for each video. It maintains this content in memory, for prior
    parsing. Only the videos with valid ids in `up_dates` are considered.
    
    Arguments
    ---------
    fpath: str
        Path to the stats files
    up_dates: dict
        Videos with valid upload dates (extracted from info files)
    
    See also
    --------
    youtime.parser.info_files
    '''
    
    xmls = {}
    with open(fpath) as vid_stats_file:
        ignore_vid = False
        for line in vid_stats_file:
            begin_matches = VID_BEGIN_MATCHER.match(line) 
            end_matches = VID_END_MATCHER.match(line)
            
            if begin_matches: #Get ID
                video_id = begin_matches.group(1)
                if video_id in up_dates and '</crawledvideoid>' not in line:
                    xmls[video_id] = []
                    ignore_vid = False
                else:
                    ignore_vid = True
            elif end_matches: #End file
                if not ignore_vid:
                    xml = ''.join(xmls[video_id])
                    del xmls[video_id]
                    xmls[video_id] = xml
            elif not ignore_vid: #Append lines
                xmls[video_id].append(line)

    return xmls

def __extract_info_from_view_graph(view_graph):
    '''
    Extracts the:
        * Initial date of the view graph
        * Final date of the view graph
        * Maximum value of the view graph (y axis)
        * Plot points for the graph
    '''
    
    view_data = VIEW_URL_MATCHER.match(view_graph)
    datespl = view_data.group(1).split('|')
    
    initial_date = mktime(strptime(datespl[1], '%m/%d/%Y'))
    final_date = mktime(strptime(datespl[len(datespl) - 1], '%m/%d/%Y'))
    top_y = round(float(view_data.group(3).split('|')[0].split(',')[-1]))
    
    view_data = [float(x) for x in view_data.group(4).split(',')]
    
    return initial_date, final_date, top_y, view_data

def __extract_info_from_view_graph_new(view_graph):
    '''
    Extracts the:
        * Initial date of the view graph
        * Final date of the view graph
        * Maximum value of the view graph (y axis)
        * Plot points for the graph
    '''
    
    view_data = VIEW_URL_MATCHER.match(view_graph)
    datespl = view_data.group(1).split('|')

    initial_date = mktime(strptime(datespl[1], '%m/%d/%y'))
    final_date = mktime(strptime(datespl[len(datespl) - 1], '%m/%d/%y'))
    top_y = round(float(view_data.group(3).split('|')[0].split(',')[-1]))
    
    view_data = [float(x) for x in view_data.group(4).split(',')]
    
    return initial_date, final_date, top_y, view_data

def __extract_points_for_small_graphs(graph):
    '''
    Extracts Y values for smaller (comments, favorites, ratings 
    and average rating) graphs.
    '''
    
    data = DATA_URL_MATCHER.match(graph)
    return  [float(x) for x in data.group(1).split(',')]

def __parse_events(event_elements):
    '''
    Parses event elements from the HTML tree returning for each event
    a list of triples:
        (ev_name, ev_date, ev_views)
    '''
    assert len(event_elements) % 3 == 0 
    events = []
    for i in xrange(0, len(event_elements), 3):
        date_element = event_elements[i + 1]
        ev_date_str = event_elements[i + 1].string.strip()
        ev_views = TO_INT(event_elements[i + 2].string)
        
        try:
            ev_date = mktime(strptime(ev_date_str, '%B %d, %Y'))
        except:
            try:
                ev_date = mktime(strptime(ev_date_str, '%b %d, %Y'))
            except:
                ev_date = None
        #Yes, this is really ugly.
        ev_name = date_element.next.next.next.next.split('-')[0].strip()
        if ev_date:
            events.append((ev_name, ev_date, ev_views))
    
    return events

def __parse_events_new(event_elements):
    '''
    Parses event elements from the HTML tree returning for each event
    a list:
        (ev_name, ev_date)
    '''
    assert len(event_elements) % 2 == 0

    events = []
    for i in range(1, len(event_elements), 2):
        date_element = event_elements[i]
        ev_date_str = event_elements[i].string.strip()

        try:
            ev_date = mktime(strptime(ev_date_str, '%m/%d/%y'))
        except:
            try:
                ev_date = mktime(strptime(ev_date_str, '%M/%d/%y'))
            except:
                ev_date = None
                
        ev_name = date_element.next.next.next.next.split('-')[0].split('&')[0].strip()
        if ev_date:
            events.append((ev_name, ev_date))
    
    return events

def _parse_html(video_id, html, up_dates):
    '''
    After separating s single video's HTML content, this method does
    the actual parsing by using beautifulsoup.
    
    Arguments
    ---------
    fpath: str
        The path of the stats file to parse
    up_dates: dict
        The upload dates for each video (video_id to int)
    '''
    
    video_data = {}
    
    #Total Views, if we can't find this. The video has no stats
    html_soup = BeautifulSoup(html)
    total_views_str = \
        html_soup.find('div', {'class':'watch-stats-title-text'}).string
    
    if not total_views_str:
        return None
    
    #Parse total views
    total_views = TO_INT(total_views_str.split()[-1])
    
    #Total comments, favorites, ratings and average rating
    base_stats = html_soup.findAll('td', 
                                   {'class':'watch-stats-sparkline-title'})
    total_comm_str = base_stats[0].string
    total_favs_str = base_stats[1].string
    total_rats_str = base_stats[2].string
    total_avgr_str = base_stats[3].string
    
    graphs = html_soup.findAll('img')
    view_graph = graphs[0]['src']
    comm_graph = graphs[1]['src']
    favs_graph = graphs[2]['src']
    rats_graph = graphs[3]['src']
    avgr_graph = graphs[4]['src']
    
    first_date, last_date, top_y, view_data = \
        __extract_info_from_view_graph(view_graph)
  
    comm_data = __extract_points_for_small_graphs(comm_graph)
    favs_data = __extract_points_for_small_graphs(favs_graph)
    rats_data = __extract_points_for_small_graphs(rats_graph)
    avgr_data = __extract_points_for_small_graphs(avgr_graph)
    
    #Extracting Events
    event_elements = html_soup.findAll('td', {'class':'watch-stats-cell'})
    events = __parse_events(event_elements)

    #Honors
    honors_str = \
        html_soup.find('span', {'class':'expander-head-stat'}).string
    honors = int(honors_str[1:-1])    
    
    #Creating return value
    video_data['VIDEO_ID'] =  video_id
    video_data['UPLOAD_DATE'] = up_dates[video_id]
    video_data['FIRST_DATE'] = first_date
    video_data['LAST_DATE'] = last_date
    
    video_data['TOTAL_VIEW'] = total_views
    video_data['TOTAL_COMM'] = TO_INT(total_comm_str.split()[-1])
    video_data['TOTAL_FAVS'] = TO_INT(total_favs_str.split()[-1])
    video_data['TOTAL_RATS'] = TO_INT(total_rats_str.split()[-1])
    video_data['TOTAL_AVGR'] = float(total_avgr_str.split()[-1])
    
    video_data['TOPY'] = top_y
    video_data['VIEW_DATA'] = view_data
    video_data['COMM_DATA'] = comm_data
    video_data['FAVS_DATA'] = favs_data
    video_data['RATS_DATA'] = rats_data
    video_data['AVGR_DATA'] = avgr_data
    
    video_data['EVENTS'] = events
    video_data['HONORS'] = honors
    
    return video_data

def _parse_html_new(video_id, html, up_dates):
    ''' 
    Similar to _parse_html
    '''
    video_data = {}
    
    #Total Views, if we can't find this. The video has no stats
    html_soup = BeautifulSoup(html)
    total_views_str = \
        html_soup.find('h4', {'class':'watch-stats-title-text'}).string
     
    if not total_views_str:
        return None
    
    #Parse total views
    total_views = TO_INT(total_views_str.split()[-1])
    
    #Total comments, favorites and ratings
    base_stats = html_soup.findAll('td', 
                                   {'class':'watch-stats-sparkline-title'})
    
    total_rats_str = base_stats[0].string
    total_comm_str = base_stats[1].string
    total_favs_str = base_stats[2].string
    
    
    graphs = html_soup.findAll('img')
    view_graph = graphs[0]['src']
    comm_graph = graphs[1]['src']
    favs_graph = graphs[2]['src']

    first_date, last_date, top_y, view_data = \
        __extract_info_from_view_graph_new(view_graph)
    
    comm_data = __extract_points_for_small_graphs(comm_graph)
    favs_data = __extract_points_for_small_graphs(favs_graph)
    
    #Extracting Events
    event_elements = html_soup.findAll('td', {'class':'watch-stats-cell'})
    events = __parse_events_new(event_elements)
    
    #Creating return value
    video_data['VIDEO_ID'] =  video_id
    video_data['UPLOAD_DATE'] = up_dates[video_id]
    video_data['FIRST_DATE'] = first_date
    video_data['LAST_DATE'] = last_date
    
    video_data['TOTAL_VIEW'] = total_views
    video_data['TOTAL_COMM'] = TO_INT(total_comm_str.split()[-1])
    video_data['TOTAL_FAVS'] = TO_INT(total_favs_str.split()[-1])
    video_data['TOTAL_RATS'] = TO_INT(total_rats_str.split()[-1])
    
    video_data['TOPY'] = top_y
    video_data['VIEW_DATA'] = view_data
    video_data['COMM_DATA'] = comm_data
    video_data['FAVS_DATA'] = favs_data
    
    video_data['EVENTS'] = events
    #video_data['HONORS'] = honors
    
    return video_data

def _parse_html_topic(video_id, html, up_dates):
    ''' 
    Similar to _parse_html but for files after 2013
    '''
    video_data = {}
    
    #Total Views, if we can't find this. The video has no stats
    html_soup = BeautifulSoup(html)
    total_views_str = \
        html_soup.find('h3').string
    
    if not total_views_str:
        return None
    
    #Parse total views
    total_views = TO_INT(total_views_str)
    
    #Total comments, favorites and ratings
    engage_stats = html_soup.find('div', 
                                  {'class':'engagement-audience'})
    
    totals = engage_stats.findAll('h4')
    
    total_comm = TO_INT(totals[0].string)
    total_favs = TO_INT(totals[1].string)
    total_likes = TO_INT(totals[2].string)
    total_dislikes = TO_INT(totals[3].string)
    
    #Graphs
    view_stats = html_soup.find('img', 
                                {'class':'stats-big-chart-expanded'})
    
    view_graph = view_stats['src']
    
    engage_graphs = engage_stats.findAll('img')
    comm_graph = engage_graphs[0]['src']
    favs_graph = engage_graphs[1]['src']
    like_graph = engage_graphs[2]['src']
    disl_graph = engage_graphs[3]['src']

    first_date, last_date, top_y, view_data = \
        __extract_info_from_view_graph_new(view_graph)
    
    comm_data = __extract_points_for_small_graphs(comm_graph)
    favs_data = __extract_points_for_small_graphs(favs_graph)
    like_data = __extract_points_for_small_graphs(like_graph)
    disl_data = __extract_points_for_small_graphs(disl_graph)
    
    #Extracting Events
    event_elements = html_soup.findAll('dd', {'class':'event'})
    events = []
    for element in event_elements:
        name_tag = element.find('span')
        if name_tag is None:
            name_tag = element.find('p')
        
        name = name_tag.string.strip()
        
        additional = ''
        additional_tag = element.find('span', {'class':'extra'})
        if additional_tag:
            link_tag = additional_tag.find('a')
            if link_tag:
                additional = link_tag.string
            else:
                additional = additional_tag.string
        
        date_tag = element.find('p', {'class':'sub-data'})
        ev_name = ' '.join((name, additional.strip()))
        
        date_str = date_tag.string.split('-')[0].strip()
        fmts = ['%b %d, %Y', '%B %d, %Y']
        ev_date = None
        for fmt in fmts:
            try:
                ev_date = mktime(strptime(date_str, fmt))
            except:
                pass
        
        events.append((ev_name, ev_date))
        
    
    #Creating return value
    video_data['VIDEO_ID'] =  video_id
    video_data['UPLOAD_DATE'] = up_dates[video_id]
    video_data['FIRST_DATE'] = first_date
    video_data['LAST_DATE'] = last_date
    
    video_data['TOTAL_VIEW'] = total_views
    video_data['TOTAL_COMM'] = total_comm
    video_data['TOTAL_FAVS'] = total_favs
    video_data['TOTAL_LIKE'] = total_likes
    video_data['TOTAL_DISL'] = total_dislikes
    
    video_data['TOPY'] = top_y
    
    video_data['VIEW_DATA'] = view_data
    video_data['COMM_DATA'] = comm_data
    video_data['FAVS_DATA'] = favs_data
    video_data['LIKE_DATA'] = like_data
    video_data['DISL_DATA'] = disl_data
    
    video_data['EVENTS'] = events
    
    return video_data

def parse_stats(fpath, up_dates):
    '''
    Parses a stats file returning a list of dictionaries each
    with the information for every video found.
    
    Arguments
    ---------
    fpath: str
        The path of the stats file to parse
    up_dates: dict
        The upload dates for each video (video_id to int)
    del_dates: dict (optional)
        The deletion dates of each video, this is useful for Youtomb.
    '''
    video_xmls = _get_video_xmls(fpath, up_dates)
    
    return_val = []
    for video_id, xml in video_xmls.iteritems():
        #The XML has only the one tag with and HTML inside
        xml_soup = BeautifulStoneSoup(xml)
        content = xml_soup.find('html_content')
        if content:
            html = content.string
        
            #We can now parse the HTML inside
            video_data = _parse_html_topic(video_id, html, up_dates)
            if video_data:
                return_val.append(video_data)
    
    return return_val