# -*- coding: utf8

from __future__ import division, print_function

from youtime import pnas

import numpy as np
import unittest

class TestCommonFunctions(unittest.TestCase):
    
    def test_group(self):
        expected = [3, 7, 11, 15, 19]
        result = pnas.group_array(np.arange(1, 11), 2)
        
        self.assertEqual(expected, [i for i in result])
        
        expected = [6, 15, 7]
        result = pnas.group_array(np.array([1, 2, 3, 4, 5, 6, 7]), 3)
        self.assertEqual(expected, [i for i in result])