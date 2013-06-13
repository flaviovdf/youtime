# -*- coding: utf8
'''
Tests the parsing done for extracting timeseries and other
information from stats files
'''

from __future__ import division, print_function

from BeautifulSoup import BeautifulStoneSoup

from time import mktime
from time import strptime

from youtime.parser import info_files
from youtime.parser import stats_files

from youtime.parser.test import ENG_INFO_FILE
from youtime.parser.test import ENG_STATS_FILE

from youtime.parser.test import PTBR_INFO_FILE
from youtime.parser.test import PTBR_STATS_FILE

from youtime.parser.test import NEW_INFO_FILE
from youtime.parser.test import NEW_INFO_FILE2

from youtime.parser.test import NEW_STATS_FILE
from youtime.parser.test import NEW_STATS_FILE2

from youtime.parser.test import TOPIC_INFO_FILE
from youtime.parser.test import TOPIC_STATS_FILE

import unittest

class TestStatsFile(unittest.TestCase):

    def test_extract_xmls_eng(self):   
        up_dates = info_files.up_dates_html_en(ENG_INFO_FILE)

        xmls = stats_files._get_video_xmls(ENG_STATS_FILE, up_dates)
        
        self.assertEqual(1, len(xmls))
        
        xml_for_vid = xmls['fhGb6qPiluE']
        lines = xml_for_vid.split('\n')
        
        self.assertEqual(375, len(lines))
        
    def test_extract_xmls_ptbr(self):
        up_dates = info_files.up_dates_html_ptbr(PTBR_INFO_FILE)
        
        xmls = stats_files._get_video_xmls(PTBR_STATS_FILE, up_dates)
        
        self.assertEqual(8, len(xmls))
        
        for xml_for_vid in xmls.values():
            lines = xml_for_vid.split('\n')
            self.assertTrue('crawledvideoid' not in lines[0])
            self.assertTrue('crawledvideoid' not in lines[-1])
            
    def test_extract_xmls_new(self):
        up_dates = info_files.up_dates_html_new(NEW_INFO_FILE)
        
        xmls = stats_files._get_video_xmls(NEW_STATS_FILE, up_dates)
        
        self.assertEqual(1, len(xmls))
        xml_for_vid = xmls['p0Jwx7n2cwc']
        lines = xml_for_vid.split('\n')
        
        self.assertEqual(315, len(lines))
        
    def test_extract_xmls_new2(self):
        up_dates = info_files.up_dates_html_new(NEW_INFO_FILE2)
        
        xmls = stats_files._get_video_xmls(NEW_STATS_FILE2, up_dates)
        self.assertEqual(1, len(xmls))
        xml_for_vid = xmls['ZzmEBY6lVAE']
        lines = xml_for_vid.split('\n')
        self.assertEqual(292, len(lines))

    def test_extract_xmls_topic(self):
        up_dates = info_files.up_dates_html_topic(TOPIC_INFO_FILE)
        
        xmls = stats_files._get_video_xmls(TOPIC_STATS_FILE, up_dates)
        self.assertEqual(1, len(xmls))
        
        xml_for_vid = xmls['zgY-cR0kv5Y']
        lines = xml_for_vid.split('\n')
        self.assertEqual(222, len(lines))

    def test_parse_html_eng(self):
        
        video_id = 'fhGb6qPiluE'
        up_dates = info_files.up_dates_html_en(ENG_INFO_FILE)
        
        xmls = stats_files._get_video_xmls(ENG_STATS_FILE, up_dates)
        xml_soup = BeautifulStoneSoup(xmls[video_id])
        html = xml_soup.find('html_content').string
        
        video_data = stats_files._parse_html(video_id, html, up_dates)
    
        expected_view_data = [0.0,8.1,9.5,11.8,15.7,19.6,23.0,23.6,24.0,24.4,
                              25.0,25.3,26.4,27.1,27.9,28.0,28.2,28.7,28.8,
                              29.6,30.2,31.1,31.5,32.1,32.9,34.6,34.6,35.0,
                              35.2,35.9,36.4,37.0,37.6,37.8,38.4,38.4,38.9,
                              40.1,40.3,41.3,41.6,41.9,42.5,43.0,43.6,44.0,
                              44.5,45.4,45.8,46.3,46.8,47.5,48.0,48.2,48.8,
                              49.7,50.7,51.1,52.5,52.5,53.3,53.9,55.6,57.3,
                              57.9,58.4,58.9,59.2,59.6,59.9,60.4,60.9,61.3,
                              61.9,62.5,62.8,63.3,63.5,63.9,64.5,65.2,69.3,
                              70.3,70.8,71.6,72.2,73.1,75.2,75.9,76.5,76.7,
                              77.5,78.6,79.4,79.6,80.0,81.4,82.5,83.0,83.3]

        expected_comm_data = [0.0,16.6,16.6,16.6,33.3,33.3,50.0,50.0,50.0,50.0,
                              50.0,50.0,50.0,66.6,66.6,66.6,66.6,66.6,66.6,66.6,
                              66.6,66.6,66.6,66.6,66.6,66.6,66.6,66.6,66.6,66.6,
                              66.6,66.6,66.6,66.6,66.6,66.6,66.6,66.6,66.6,66.6,
                              66.6,66.6,66.6,66.6,66.6,66.6,66.6,66.6,66.6,66.6,
                              66.6,66.6,66.6,66.6,66.6,66.6,100.0,100.0,100.0,
                              100.0,100.0,100.0,100.0,100.0,100.0,100.0,100.0,
                              100.0,100.0,100.0,100.0,100.0,100.0,100.0,100.0,
                              100.0,100.0,100.0,100.0,100.0,100.0,100.0,100.0,
                              100.0,100.0,100.0,100.0,100.0,100.0,100.0,100.0,
                              100.0,100.0,100.0,100.0,100.0,100.0,100.0,100.0,
                              100.0]

        expected_favs_data = [0] * 100
        
        expected_rats_data = [0.0,0.0,0.0,0.0,50.0,50.0,50.0,50.0,50.0,50.0,
                              50.0,50.0,50.0,100.0,100.0,100.0,100.0,100.0,
                              100.0,100.0,100.0,100.0,100.0,100.0,100.0,100.0,
                              100.0,100.0,100.0,100.0,100.0,100.0,100.0,100.0,
                              100.0,100.0,100.0,100.0,100.0,100.0,100.0,100.0,
                              100.0,100.0,100.0,100.0,100.0,100.0,100.0,100.0,
                              100.0,100.0,100.0,100.0,100.0,100.0,100.0,100.0,
                              100.0,100.0,100.0,100.0,100.0,100.0,100.0,100.0,
                              100.0,100.0,100.0,100.0,100.0,100.0,100.0,100.0,
                              100.0,100.0,100.0,100.0,100.0,100.0,100.0,100.0,
                              100.0,100.0,100.0,100.0,100.0,100.0,100.0,100.0,
                              100.0,100.0,100.0,100.0,100.0,100.0,100.0,100.0,
                              100.0,100.0]

        expected_avgr_data = [0] * 4 + [100] * 96

        expected_upload_date = up_dates[video_id]
        expected_last_date = \
            mktime(strptime('April 14, 2010', '%B %d, %Y'))
         
        self.assertEqual(740, video_data['TOTAL_VIEW'])
        self.assertEqual(5, video_data['TOTAL_COMM'])
        self.assertEqual(0, video_data['TOTAL_FAVS'])
        self.assertEqual(2, video_data['TOTAL_RATS'])
        self.assertEqual(5.0, video_data['TOTAL_AVGR'])
        
        self.assertEqual(expected_view_data, video_data['VIEW_DATA'])
        self.assertEqual(expected_comm_data, video_data['COMM_DATA'])
        self.assertEqual(expected_favs_data, video_data['FAVS_DATA'])
        self.assertEqual(expected_rats_data, video_data['RATS_DATA'])
        self.assertEqual(expected_avgr_data, video_data['AVGR_DATA'])
        
        self.assertEqual(expected_upload_date, video_data['UPLOAD_DATE'])
        self.assertEqual(expected_upload_date - 86400, video_data['FIRST_DATE'])
        self.assertEqual(expected_last_date, video_data['LAST_DATE'])
        
        self.assertEqual(888, video_data['TOPY'])
        self.assertEqual(0, video_data['HONORS'])
        self.assertEqual(10, len(video_data['EVENTS']))

    def test_parse_html_new(self):
        
        video_id = 'p0Jwx7n2cwc'
        up_dates = info_files.up_dates_html_new(NEW_INFO_FILE)
        
        xmls = stats_files._get_video_xmls(NEW_STATS_FILE, up_dates)
        xml_soup = BeautifulStoneSoup(xmls[video_id])
        html = xml_soup.find('html_content').string

        video_data = stats_files._parse_html_new(video_id, html, up_dates)
        
        expected_view_data = [0.0, 2.9, 3.1, 3.2, 3.8, 3.9, 4.0, 4.1, 4.5, 4.7, 
                              4.8, 5.0, 5.0, 5.5, 6.0, 6.8, 7.7, 8.3, 9.0, 9.5,
                              10.5, 11.0, 11.7, 12.7, 13.9, 15.1, 15.7, 16.4,
                              18.4, 19.6, 20.4, 21.3, 21.9, 22.6, 23.7, 24.5, 
                              25.5, 27.2, 28.8, 30.4, 32.0, 33.6, 34.7, 35.8, 
                              36.3, 36.9, 37.8, 38.9, 39.9, 40.4, 41.7, 42.7,
                              43.8, 44.6, 45.0, 45.8, 47.5, 48.8, 50.0, 50.7,
                              51.7, 52.6, 53.4, 54.3, 55.3, 56.8, 58.2, 59.1,
                              60.0, 61.4, 62.8, 64.5, 65.6, 66.7, 67.5, 68.2,
                              69.3, 70.0, 70.6, 72.3, 73.1, 73.8, 74.7, 75.7,
                              76.6, 77.2, 77.4, 77.7, 78.3, 78.8, 79.3, 79.7,
                              80.0, 80.8, 80.9, 81.4, 81.8, 82.1, 82.4, 83.3]
        
        expected_comm_data = [0] * 100
        
        expected_favs_data = [0] * 100
        
        expected_upload_date = up_dates[video_id]
        expected_last_date = \
            mktime(strptime('February 16, 2012', '%B %d, %Y'))
        
        self.assertEqual(1019, video_data['TOTAL_VIEW'])
        self.assertEqual(0, video_data['TOTAL_COMM'])
        self.assertEqual(0, video_data['TOTAL_FAVS'])
        self.assertEqual(0, video_data['TOTAL_RATS'])
        
        self.assertEqual(expected_view_data, video_data['VIEW_DATA'])
        self.assertEqual(expected_comm_data, video_data['COMM_DATA'])
        self.assertEqual(expected_favs_data, video_data['FAVS_DATA'])

        self.assertEqual(expected_upload_date, video_data['UPLOAD_DATE'])
        self.assertEqual(expected_upload_date - 86400, video_data['FIRST_DATE'])
        self.assertEqual(expected_last_date, video_data['LAST_DATE'])
        
        self.assertEqual(1212, video_data['TOPY'])
        self.assertEqual(10, len(video_data['EVENTS']))
    
    def test_parse_topics(self):
        
        video_id = 'zgY-cR0kv5Y'
        up_dates = info_files.up_dates_html_topic(TOPIC_INFO_FILE)
        
        xmls = stats_files._get_video_xmls(TOPIC_STATS_FILE, up_dates)
        xml_soup = BeautifulStoneSoup(xmls[video_id])
        html = xml_soup.find('html_content').string

        video_data = stats_files._parse_html_topic(video_id, html, up_dates)
        
        expected_view_data = [0.0, 28.6, 50.4, 54.9, 59.8, 62.3, 64.0, 65.9, 
                              67.7, 69.2, 69.9, 70.3, 70.7, 71.1, 71.4, 71.7, 
                              72.0, 72.2, 72.4, 72.5, 72.8, 72.9, 73.0, 73.2, 
                              73.5, 73.8, 73.9, 74.1, 74.4, 74.6, 74.8, 75.2, 
                              75.4, 75.6, 75.8, 75.9, 76.0, 76.2, 76.4, 76.6, 
                              76.8, 76.9, 77.0, 77.1, 77.2, 77.3, 77.4, 77.5, 
                              77.6, 77.8, 78.0, 78.1, 78.2, 78.2, 78.3, 78.4, 
                              78.5, 78.5, 78.6, 78.7, 78.9, 79.2, 79.4, 79.5, 
                              79.6, 79.7, 79.8, 79.9, 80.0, 80.0, 80.0, 80.1, 
                              80.2, 80.3, 80.4, 80.5, 80.6, 80.8, 80.9, 81.0, 
                              81.2, 81.3, 81.4, 81.5, 81.7, 81.8, 82.0, 82.1, 
                              82.3, 82.5, 82.6, 82.8, 82.9, 83.0, 83.1, 83.2, 
                              83.3, 83.3, 83.3, 83.3]
        
        expected_upload_date = up_dates[video_id]
        expected_last_date = \
            mktime(strptime('Jun 12, 2013', '%b %d, %Y'))
        
        self.assertEqual(13236, video_data['TOTAL_VIEW'])
        self.assertEqual(51, video_data['TOTAL_COMM'])
        self.assertEqual(3, video_data['TOTAL_FAVS'])
        self.assertEqual(26, video_data['TOTAL_LIKE'])
        self.assertEqual(1, video_data['TOTAL_DISL'])
        
        self.assertEqual(expected_view_data, video_data['VIEW_DATA'])
        self.assertEqual(54.9, video_data['COMM_DATA'][1])
        self.assertEqual(66.6, video_data['FAVS_DATA'][1])
        self.assertEqual(65.3, video_data['LIKE_DATA'][1])
        self.assertEqual(100.0, video_data['DISL_DATA'][1])
        
        self.assertEqual(expected_upload_date, video_data['UPLOAD_DATE'])
        self.assertEqual(expected_upload_date, video_data['FIRST_DATE'])
        self.assertEqual(expected_last_date, video_data['LAST_DATE'])
        
        self.assertEqual(15862, video_data['TOPY'])
        self.assertEqual(9, len(video_data['EVENTS']))
    
    def test_parse_html_new2(self):
        
        video_id = 'ZzmEBY6lVAE'
        up_dates = info_files.up_dates_html_new(NEW_INFO_FILE2)
        
        xmls = stats_files._get_video_xmls(NEW_STATS_FILE2, up_dates)
        xml_soup = BeautifulStoneSoup(xmls[video_id])
        html = xml_soup.find('html_content').string
    
        video_data = stats_files._parse_html_new(video_id, html, up_dates)
        
        expected_view_data = [0.0,3.0,4.3,5.5,6.1,6.1,6.1,6.1,6.1,6.7,
                              7.9,9.1,11.6,13.4,14.0,14.0,15.8,15.8,15.8,
                              15.8,15.8,15.8,15.8,15.8,15.8,15.8,15.8,15.8,
                              17.0,18.2,19.5,21.3,23.1,25.5,27.4,28.0,29.8,
                              32.2,33.5,34.1,34.7,35.3,35.9,37.1,41.4,43.2,
                              44.4,45.0,46.2,48.7,49.9,53.5,55.4,56.6,57.2,
                              57.8,60.8,61.4,63.3,65.1,65.7,66.3,68.7,69.3,
                              69.3,70.0,70.6,70.6,70.6,70.6,71.8,71.8,71.8,
                              71.8,72.4,73.6,74.2,74.2,74.2,74.2,74.8,74.8,
                              75.4,75.4,76.6,77.3,79.1,79.7,79.7,80.3,80.9,
                             81.5,82.1,82.1,82.1,82.1,82.7,82.7,83.3,83.3]
        
        expected_comm_data = [0] * 100
        
        expected_favs_data = [0] * 100
        
        expected_upload_date = up_dates[video_id]
        expected_last_date = \
            mktime(strptime('February 16, 2012', '%B %d, %Y'))
        
        self.assertEqual(138, video_data['TOTAL_VIEW'])
        self.assertEqual(0, video_data['TOTAL_COMM'])
        self.assertEqual(0, video_data['TOTAL_FAVS'])
        self.assertEqual(1, video_data['TOTAL_RATS'])
        
        self.assertEqual(expected_view_data, video_data['VIEW_DATA'])
        self.assertEqual(expected_comm_data, video_data['COMM_DATA'])
        self.assertEqual(expected_favs_data, video_data['FAVS_DATA'])

        self.assertEqual(expected_upload_date, video_data['UPLOAD_DATE'])
        self.assertEqual(expected_upload_date, video_data['FIRST_DATE'])
        self.assertEqual(expected_last_date, video_data['LAST_DATE'])
        
        self.assertEqual(164.0, video_data['TOPY'])
        self.assertEqual(10, len(video_data['EVENTS']))

    def test_parse_html_ptbr_ok(self):
        up_dates = info_files.up_dates_html_ptbr(PTBR_INFO_FILE)
        ok_id = 'l4eiwiLvp40'
        
        xmls = stats_files._get_video_xmls(PTBR_STATS_FILE, up_dates)
        
        xml_soup = BeautifulStoneSoup(xmls[ok_id])
        html = xml_soup.find('html_content').string
        video_data = stats_files._parse_html(ok_id, html, up_dates)
        self.assertTrue(len(video_data['EVENTS']) > 0)
        
    def test_parse_html_broken(self):
        broken_id = 'lYQmLjWubaI'
        up_dates = info_files.up_dates_html_ptbr(PTBR_INFO_FILE)
        
        xmls = stats_files._get_video_xmls(PTBR_STATS_FILE, up_dates)
        
        xml_soup = BeautifulStoneSoup(xmls[broken_id])
        html = xml_soup.find('html_content').string
        self.assertTrue(stats_files._parse_html(broken_id, html, up_dates) \
                        is None)
                
if __name__ == "__main__":
    unittest.main()