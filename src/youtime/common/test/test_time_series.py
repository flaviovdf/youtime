# -*- coding: utf-8

from youtime.common import time_series

import numpy as np
import unittest

class TestTSeries(unittest.TestCase):

    def test_cov(self):
        
        y = np.array([6.7, 5.3, 3.3, 6.7, 3.3, 4.7, 4.7, 6.7, 3.3, 6.7])
        
        cov_3 = time_series.auto_cov(y, 3)
        self.assertAlmostEqual(cov_3, -0.04848)

        cov_0 = time_series.auto_cov(y, 0)
        self.assertAlmostEqual(cov_0, 2.0304)


    def test_corr(self):

        y = np.array([6.7, 5.3, 3.3, 6.7, 3.3, 4.7, 4.7, 6.7, 3.3, 6.7])
        corr_3 = time_series.auto_corr(y, 3)
        corr_1 = time_series.auto_corr(y, 1)

        self.assertAlmostEqual(corr_3, -0.0238770685579)
        self.assertAlmostEqual(corr_1, -0.552088258471)

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()