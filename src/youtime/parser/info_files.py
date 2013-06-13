# -*- coding: utf8
'''
Functions for parsing HTML info files. What we want from these files
is the video's upload date. The rest of the information we can remove
from stats files.
'''

from __future__ import division, print_function

from BeautifulSoup import BeautifulSoup

from time import mktime
from time import strptime

from youtime.common import log
from youtime.parser import END_OF_VID_RE
from youtime.parser import VIDEOID_RE

import re

UPDATE_ENG = r'\s+<span class="watch-video-date">(.*?)</span>\s*'
UPDATE_PTBR = r'\s*<span class="watch-video-added post-date">(.*?)</span><br/>\s*'

USERNAME_RE = r"\s*'VIDEO_USERNAME':\s*'(.*?)'.*"

VIDEOID_MATCHER = re.compile(VIDEOID_RE)
EOV_MATCHER = re.compile(END_OF_VID_RE)

UPDATE_ENG_MATCHER = re.compile(UPDATE_ENG)
UPDATE_PTBR_MATCHER = re.compile(UPDATE_PTBR)

USERNAME_MATCHER = re.compile(USERNAME_RE)

def _real_extract(fpath, update_matcher):
    '''
    Parses an info HTML file and returns the upload dates of each
    video found.
    
    Arguments
    ---------
    fpath: str
        Path to the file
    
    update_matcher: a regular expression matcher
        A matcher for the video date. This changes based on the locale
        used to download the video.
    '''
    
    processed = 0
    upload_dates = {}
    
    with open(fpath) as info_file:
        vid_id = None
        up_date = None
        
        for line in info_file:
            match = VIDEOID_MATCHER.match(line)
            if match:
                vid_id = match.group(1)
                log('Found video: %s'%vid_id)

            match = update_matcher.match(line)
            if match:
                up_date = match.group(1)

            match = EOV_MATCHER.match(line)
            if match:
                processed += 1
                if up_date:
                    assert(vid_id)
                    upload_dates[vid_id] = up_date
                    log_msg = 'Processed %d videos, last id OK! %s'
                else:
                    log_msg = 'Processed %d videos, id %s was an error page'
                    
                log(log_msg %(processed, vid_id))
                vid_id = None
                up_date = None

    return upload_dates
                    
def up_dates_html_en(fpath):
    '''
    Parses an info HTML file and returns the upload dates of each
    video found. This method is used for files down loaded using
    the English locale.
    
    Arguments
    ---------
    fpath: str
        Path to the file
    '''
    
    dates = _real_extract(fpath, UPDATE_ENG_MATCHER)

    #Converting dates
    return_val = {}
    for video_id, str_date in dates.iteritems():
        #For some weird reason, dates can have two formats.
        try:
            up_date = mktime(strptime(str_date, '%B %d, %Y'))
        except:
            up_date = mktime(strptime(str_date, '%b %d, %Y'))
        
        return_val[video_id] = up_date

    return return_val

def up_dates_html_ptbr(fpath):
    '''
    Parses an info HTML file and returns the upload dates of each
    video found. This method is used for files down loaded using
    the PT-BR locale.
    
    Arguments
    ---------
    fpath: str
        Path to the file
    '''
    
    meses = {'janeiro':1,
             'fevereiro':2,
             'março':3,
             'abril':4,
             'maio':5,
             'junho':6,
             'julho':7,
             'agosto':8,
             'setembro':9,
             'outubro':10,
             'novembro':11,
             'dezembro':12}
            
    

    dates = _real_extract(fpath, UPDATE_PTBR_MATCHER)
    return_val = {}
    for video_id, str_date in dates.iteritems():
        spl = str_date.split(' de ')
        
        spl[1] = str(meses[spl[1]])
          
        day_month_year =  ' '.join(spl).lower()
        up_date = mktime(strptime(day_month_year, '%d %m %Y'))

        return_val[video_id] = up_date
        
    return return_val

def up_dates_html_new (fpath):
    '''
    Parses an info HTML file and return the upload date and the videoid found.
    
    Arguments
    ---------
    fpath: str
        Path to the file
    ''' 
    
    soup = BeautifulSoup(open(fpath))
    str_date = soup.find(id="eow-date").string

    f = open(fpath)
    video_id = f.readline().strip().split('=')[1].split()[0]
    f.close()
    
    #converting date
    up_date = mktime(strptime(str_date, '%b %d, %Y'))
    return_val = {}
    return_val[video_id] = up_date
    
    return return_val
    
     
def _get_video_htmls(fpath):
    '''
    This method filters out only the html content of the info file
    for each video. It maintains this content in memory, for prior
    parsing.
    
    Arguments
    ---------
    fpath: str
        Path to the stats files
    
    See also
    --------
    youtime.parser.stat_files
    '''
    
    tmp_lists = {}
    videos = {}
    user_names = {}
    
    with open(fpath) as info_file:
        curr_video_id = None
        for line in info_file:
            
            begin_match = VIDEOID_MATCHER.match(line)
            end_match = EOV_MATCHER.match(line)
            
            if begin_match:
                curr_video_id = begin_match.group(1)
                tmp_lists[curr_video_id] = []
            elif end_match:
                #replace with actual text
                videos[curr_video_id] = ''.join(tmp_lists[curr_video_id])
                del tmp_lists[curr_video_id]
            elif curr_video_id in tmp_lists:
                tmp_lists[curr_video_id].append(line)
                
                matches = USERNAME_MATCHER.match(line)
                if matches:
                    user_names[curr_video_id] = matches.group(1)
                
    return videos, user_names

def category_tags_user_en(fpath):
    '''
    Parses an info HTML file and returns the name of the uploader,
    the category and tags of a video as a single string per video.
    
    Arguments
    ---------
    fpath: str
        Path to the file
    '''
    
    htmls, user_names = _get_video_htmls(fpath)
    
    return_val = {}
    for vid_id, html in htmls.iteritems():
        html_soup = BeautifulSoup(html)
        
        aux = html_soup.findAll('div', {'id':'watch-category'})
        if not aux:
            continue
        
        category_div = \
            aux[0].find('a')

        aux = html_soup.findAll('div', {'id':'watch-tags'})
        if not aux:
            #        <div id="watch-video-tags" class="floatL">
            aux = html_soup.findAll('div', {'id':'watch-video-tags'})
            
        tags_divs = \
            aux[0].findAll('a')
        category = category_div.string.replace(' ', '')

        tags = set()
        for tag_div in tags_divs:
            if tag_div.string is not None:
                tags.add(tag_div.string)
        
        return_val[vid_id] = '%s %s %s' % (user_names[vid_id], category, 
                                           ' '.join(tags))

    return return_val

def category_tags_user_new (fpath):
    '''
    Parses an info HTML file and returns the name of the uploader,
    the category and tags of a video as a single string per video.
    
    Arguments
    ---------
    fpath: str
    Path to the file
    '''
    return_val = {}
    f = open(fpath)
    video_id = f.readline().strip().split('=')[1].split()[0]
    f.close()
    
    soup = BeautifulSoup(open(fpath))
    user_name = str(soup.find('span',{"class":"yt-uix-button-group"}).find('span',{"class":"yt-uix-button-content"}).string).strip()
    
    tags = []
    for line in soup.findAll (id="eow-tags"):
        for l in line.findAll('a'):
            tags.append(l.string)
    
    for line in soup.findAll(id="eow-category"):
        for l in line.findAll('a'):
            category = l.string.strip()
            
    return_val[video_id] = '%s %s %s' % (user_name, category, ' '.join(tags))       

    return return_val