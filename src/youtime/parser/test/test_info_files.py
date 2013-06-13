# -*- coding: utf8
'''Contains tests for function which parses info (video html) files.'''

from __future__ import division, print_function

from time import mktime
from time import strptime

from youtime.parser import info_files
from youtime.parser.test import ENG_INFO_FILE
from youtime.parser.test import ENG_INFO_FILE2
from youtime.parser.test import NEW_INFO_FILE
from youtime.parser.test import NEW_INFO_FILE2
from youtime.parser.test import PTBR_INFO_FILE
from youtime.parser.test import TOPIC_INFO_FILE

import unittest

class TestInfoFile(unittest.TestCase):

    def test_ptbr(self):
        up_dates = info_files.up_dates_html_ptbr(PTBR_INFO_FILE)
        self.assertEqual(8, len(up_dates))
        
        expected_secs = mktime(strptime('January 20, 2008', '%B %d, %Y'))
        self.assertEqual(expected_secs, up_dates['l4eiwiLvp40'])

    def test_eng(self):
        expected = 'January 21, 2009'
        expected_secs = mktime(strptime(expected, '%B %d, %Y'))
        up_dates = info_files.up_dates_html_en(ENG_INFO_FILE)
        self.assertEqual(expected_secs, up_dates['fhGb6qPiluE'])

    def test_new(self):
        expected = 'Jul 30, 2008'
        expected_secs = mktime(strptime(expected, '%b %d, %Y'))
        up_dates = info_files.up_dates_html_new(NEW_INFO_FILE)
        self.assertEqual(expected_secs, up_dates['p0Jwx7n2cwc'])

    def test_new2(self):
        expected = 'Jul 30, 2008'
        expected_secs = mktime(strptime(expected, '%b %d, %Y'))
        up_dates = info_files.up_dates_html_new(NEW_INFO_FILE2)
        self.assertEqual(expected_secs, up_dates['ZzmEBY6lVAE'])
    
    def test_topic(self):
        expected = 'Mar 5, 2013'
        expected_secs = mktime(strptime(expected, '%b %d, %Y'))
        up_dates = info_files.up_dates_html_topic(TOPIC_INFO_FILE)
        self.assertEqual(expected_secs, up_dates['zgY-cR0kv5Y'])
    
    def test_tags_en(self):
        expected = 'valentinchris Sports cioncan maria'
        info = info_files.category_tags_user_en(ENG_INFO_FILE)
        self.assertEqual(expected, info['fhGb6qPiluE'])

    def test_tags_en2(self):
        expected = \
                'jonlajoie Comedy normal cup song girls f#%k high as rap' 
        expected += ' Lajoie guy everyday Jon'
        
        info = info_files.category_tags_user_en(ENG_INFO_FILE2)
        self.assertEqual(expected, info['5PsnxDQvQpw'])
        
    def teste_tags_new(self):
        expected = 'alexhui123 Sports los narkillos de ka\xc3\xb1ada'
        info = info_files.category_tags_user_new(NEW_INFO_FILE)
        self.assertEqual(expected, info['p0Jwx7n2cwc'])
        
    def teste_tags_new2(self):
        expected = 'miley1938 Education learning to make sandwish'
        info = info_files.category_tags_user_new(NEW_INFO_FILE2)
        self.assertEqual(expected, info['ZzmEBY6lVAE'])
 
if __name__ == "__main__":
    unittest.main()