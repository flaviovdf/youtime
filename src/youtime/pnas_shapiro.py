# -*- coding: utf8
'''
This class performs powerlaw fits do time series which do not follow a Poisson 
distribution. For each time series, it determines the best powerlaw fit shape 
parameter.
'''
from __future__ import division, print_function

from scipy import stats

from vod.fileutil import write_xy_to_file
from vod.mapreducescript import BaseMapper
from vod.mapreducescript import BaseReducer
from vod.stats.fit import least_square_powerlaw

from youtime import YoutimeH5Runner
from youtime.pnas import group_array
from youtime.pnas import is_poisson
from youtime.common.constants import SIGNIFICANCE
from youtime.common.dao import VideoDAO
from youtime.mapred.ig import PyTablesDaoIterator

import numpy as np
import os
import sys

def crane_sornet(ypts):
    y = np.asarray(ypts)
    y = y[np.nonzero(y)]
    x = np.arange(1, len(y) + 1)
    
    fit_res = least_square_powerlaw(x, y)
    estimate = fit_res[0]
    residuals = fit_res[1]

    if np.isnan(estimate[0]) or np.isnan(estimate[1]): #Unable to fit
        return (-1, 0)

    e_theta = estimate[0]
    if residuals.shape[0] >= 3:
        p_norm = stats.shapiro(residuals / y)[1]
        return (-1 * e_theta, p_norm)
    else:
        return (-1, 0)

class PNASShapiroMapper(BaseMapper):

    def __init__(self, days): 
        self.days = days

    def _map(self, key, item):
        views = item[VideoDAO.VIEW_DATA_INTERP]
        if len(views) < 100 or len(views) > 200:
            return None

        total = np.sum(views)
        poiss_test = group_array(views, self.days)
        is_poiss = is_poisson(poiss_test)[1] or len(poiss_test) < 4
        unknown_or_poiss = (-1, 0)
        if is_poiss: #Poisson
            return unknown_or_poiss
        else:
            max_pos = np.argmax(poiss_test)
            max_win = poiss_test[max_pos]
            pl_test = poiss_test[max_pos:]
                
            if len(pl_test) <= 10: #We need at least 10 days after the peak
                return unknown_or_poiss
        
            peak_f = max_win / total
            t, shapiro_p = (-1, 0, 0)
            for i in xrange(len(pl_test), 9, -1):
                t, shapiro_p = crane_sornet(pl_test[:i])
                if shapiro_p > SIGNIFICANCE and t >= 0: #FIT!
                    return t, peak_f
    
            return unknown_or_poiss
        
class PNASShapiroReducer(BaseReducer):

    def __init__(self, outf, append):
        assert(os.path.isdir(outf))
        self.data = []
        self.peak = []
        self.outf = outf
        self.append = append

    def _reduce(self, key, value):
        if value[0] >= 0: #Has a class
            self.data.append(value[0])
            self.peak.append(value[1])

    def close(self):
        write_xy_to_file(self.data, self.peak, 
                         os.path.join(self.outf, 'pnas_exps_raw'
                                      +self.append+'.txt'))


class PNASShapiro(YoutimeH5Runner):

    def __init__(self, name, description):
        super(PNASShapiro, self).__init__(name, description)
        self.mapper_obj = None
        self.reducer_obj = None
    
    def item_generator(self):
        return self.igen
    
    def mapper(self):
        return self.mapper_obj
    
    def reducer(self):
        return self.reducer_obj
    
    def add_custom_aguments(self, parser):
        super(PNASShapiro, self).add_custom_aguments(parser)
        parser.add_argument("days", type=int,
                            help="How many days in each window")
    
    def finalize(self):
        self.reducer_obj.close()
    
    def setup(self, arg_vals):
        append = str(arg_vals.days)
        
        self.igen = PyTablesDaoIterator(arg_vals.in_file, arg_vals.table)
        self.mapper_obj = PNASShapiroMapper(arg_vals.days)
        self.reducer_obj = PNASShapiroReducer(arg_vals.outf, append)
        
if __name__ == '__main__':
    runner = PNASShapiro(sys.argv[0], __doc__)
    runner(sys.argv[1:])