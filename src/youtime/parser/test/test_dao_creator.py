#-*- coding: utf8
'''
Tests for the conversion of parsed data to the time series and points
which will actually be analyzed.
'''
from __future__ import division, print_function

from BeautifulSoup import BeautifulStoneSoup

from youtime.common.constants import DAY
from youtime.common.dao import VideoDAO

from youtime.parser import dao_creator
from youtime.parser import info_files
from youtime.parser import stats_files
from youtime.parser.test import ENG_INFO_FILE
from youtime.parser.test import ENG_STATS_FILE

import numpy as np

import unittest

class TestCreator(unittest.TestCase):

    def test_percentual_to_absolute(self):
        converted = \
            dao_creator._percentual_data_to_absolute(100, [])
        self.assertEqual([], converted)
        
        converted = \
            dao_creator._percentual_data_to_absolute(98, [50])
        self.assertEqual([49], converted)
            
        converted = \
            dao_creator._percentual_data_to_absolute(100, [0, 33, 50, 99, 100])
        self.assertEqual([0, 33, 50, 99, 100], converted)

        converted = \
            dao_creator._percentual_data_to_absolute(66, [0, 33, 50, 100])
        self.assertEqual([0, 22, 33, 66], converted)

    def test_get_days_in_range(self):
        #Less than 100
        days, points = dao_creator._get_days_in_range(DAY, DAY * 10, 10)
        self.assertEqual(DAY * 10, points[-1])
        self.assertEqual(DAY, points[0])
        self.assertEqual(10, len(points))
        self.assertEqual(10, days)
        
        #Exactly 100
        days, points = dao_creator._get_days_in_range(DAY, DAY * 100, 100)
        self.assertEqual(DAY * 100, points[-1])
        self.assertEqual(DAY, points[0])
        self.assertEqual(100, len(points))
        self.assertEqual(100, days)
        
        #More than 100
        days, points = dao_creator._get_days_in_range(DAY, DAY * 300, 100)
        self.assertEqual(DAY * 300, points[-1])
        self.assertEqual(DAY, points[0])
        self.assertEqual(100, len(points))
        self.assertEqual(300, days)
        
        delta = points[1] - points[0]
        for i in xrange(2, len(points)):
            self.assertAlmostEqual(delta, points[i] - points[i - 1])

    def test_to_delta(self):
        self.assertEqual([0, 1, 2, 3, 4], 
                         dao_creator._to_delta([0, 1, 3, 6, 10]))

    def test_all(self):
        video_id = 'fhGb6qPiluE'
        up_dates = info_files.up_dates_html_en(ENG_INFO_FILE)
        
        xmls = stats_files._get_video_xmls(ENG_STATS_FILE, up_dates)
        xml_soup = BeautifulStoneSoup(xmls[video_id])
        html = xml_soup.find('html_content').string
        
        video_data = stats_files._parse_html(video_id, html, up_dates)
        raw = dao_creator.create(video_data)
        
        self.assertAlmostEqual(740, raw[VideoDAO.TOTAL_VIEW])
        self.assertAlmostEqual(5, raw[VideoDAO.TOTAL_COMM])
        self.assertAlmostEqual(0, raw[VideoDAO.TOTAL_FAVS])

        self.assertAlmostEqual(740, raw[VideoDAO.VIEW_DATA_INTERP][1:].sum())
        self.assertAlmostEqual(5, raw[VideoDAO.COMM_DATA_INTERP][1:].sum())
        self.assertAlmostEqual(0, raw[VideoDAO.FAVS_DATA_INTERP][1:].sum())
        
        self.assertAlmostEqual(740, raw[VideoDAO.VIEW_DATA_ORIG][1:].sum())
        self.assertAlmostEqual(5, raw[VideoDAO.COMM_DATA_ORIG][1:].sum())
        self.assertAlmostEqual(0, raw[VideoDAO.FAVS_DATA_ORIG][1:].sum())
                
        for key in raw:
            if 'ORIG' in key:
                self.assertEqual(101, len(raw[key]))
                self.assertEqual(100, raw[key][0])
            elif 'INTERP' in key:
                self.assertEqual(2001, len(raw[key]))
                self.assertEqual(450, raw[key][0])
                self.assertFalse(raw[key][452:].any())
            elif 'EVENT' in key:
                self.assertEqual(11, len(raw[key]))
                if raw[key].dtype == np.int32:
                    self.assertEqual(10, raw[key][0])
                else:
                    self.assertEqual('10', raw[key][0])
        
        #Simple creation of object
        VideoDAO(raw)
        
if __name__ == "__main__":
    unittest.main()