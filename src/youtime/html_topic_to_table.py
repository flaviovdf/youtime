# -*- coding: utf-8
'''
Extracts HTML files to time series tables directly. Can only be used in the 2013
crawl.
'''
from __future__ import division, print_function

from BeautifulSoup import BeautifulStoneSoup

from parser import stats_files

import glob
import multiprocessing
import numpy as np
import os
import plac
import sys

def extract(fpath):
    first_idx = fpath.find('-')
    last_idx = fpath.rfind('-')
    vid_id = fpath[first_idx + 1:last_idx]
    
    up_dates = {}
    up_dates[vid_id] = 0
     
    try:
        xmls = stats_files._get_video_xmls(fpath, up_dates)
        xml_soup = BeautifulStoneSoup(xmls[vid_id])
        html = xml_soup.find('html_content').string

        data = stats_files._parse_html_topic(vid_id, html, up_dates)
        top = data['TOPY']
        view_data = data['VIEW_DATA']

        arr = [0] + [top * x * 0.01 for x in view_data]
        if len(arr) == 100:
            arr += [arr[-1]]
        
        arr = np.round(np.diff(arr))
        
        assert len(arr) == 100
        
        tseries = ' '.join(str(x) for x in arr)
        print('parsed fpath', fpath, len(arr), file=sys.stderr)
        return vid_id, tseries
    except Exception as e:
        print('error at fpath', fpath, e, file=sys.stderr)
        return None

def main(input_folder):
    
    fpaths = glob.glob(os.path.join(input_folder, '*-stats.html'))
    pool = multiprocessing.Pool()
    results = pool.map(extract, fpaths, chunksize=1000)
    
    for result in sorted(results):
        if result:
            print(result[0], result[1])

    pool.close()
    pool.join()

if __name__ == '__main__':
    sys.exit(plac.call(main))
