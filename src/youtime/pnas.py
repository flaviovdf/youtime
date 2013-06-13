# -*- coding: utf-8
'''
This script plots the number of videos which belong to the viral, junk
and quality classes. It makes use of a Poisson test to filter out memoryless
time series and then determines the class by the peak view.
'''
from __future__ import division, print_function

from vod.mapreducescript import BaseMapper
from vod.mapreducescript import BaseReducer
from vod.stats.gof import chisq_poisson

from vodstats.plot.plot2d import Bars, barplot

from youtime import YoutimeH5Runner
from youtime.common.constants import SIGNIFICANCE
from youtime.common.dao import VideoDAO
from youtime.mapred.ig import PyTablesDaoIterator

import numpy as np
import os
import sys

#Constants for each PNAS class
UNKNOWN = 0
VIRAL = 1
QUALITY = 2
JUNK = 3
MEMORYLESS = 4

NAMES = {UNKNOWN: 'unknown',
         VIRAL: 'viral',
         QUALITY: 'quality',
         JUNK: 'junk',
         MEMORYLESS: 'memless'}

def group_array(data_array, group_size):
    return_size = np.ceil(data_array.shape[0] / group_size)
    return_val = np.zeros(return_size, dtype=data_array.dtype)

    from_ = 0    
    for i in xrange(int(return_size)):
        end = group_size * (i + 1)
        return_val[i] = data_array[from_:end].sum()
        from_ = end
    
    return return_val

def is_poisson(data_array):
    max_window = np.max(data_array, axis=0)
    frac = max_window / data_array.sum()
        
    pval = chisq_poisson(np.array(data_array, 'i'))[1]
    if pval <= SIGNIFICANCE: #Not poisson
        return (False, frac)
    else:
        return (True, frac)

def get_class(data_array, threshold):
    if len(data_array) < 4: #Minimum number of points
        return UNKNOWN
    
    is_poiss, peak_views = is_poisson(data_array)
    if is_poiss:
        return MEMORYLESS
    else:
        if peak_views <= threshold:
            return VIRAL
        elif peak_views <= (1 - threshold):
            return QUALITY
        else:
            return JUNK

class PNASMapper(BaseMapper):

    def __init__(self, days): 
        self.days = days

    def _map(self, key, item):
        views = item[VideoDAO.VIEW_DATA_INTERP]
        
        to_test = group_array(views, self.days)
        threshold_vals = np.arange(0.05, 0.55, 0.05)
        return_val = np.zeros(threshold_vals.shape[0])
        
        for i, threshold in enumerate(threshold_vals):
            cls = get_class(to_test, threshold)
            return_val[i] = cls
        
        return return_val
    
class PNASReducer(BaseReducer):

    def __init__(self, outf, append):
        assert(os.path.isdir(outf))
        self.outf = outf
        self.append = append
        self.total = 0
        self.known = 0
        self.vid_classes = {}
        
        #Used for plotting
        for t in np.arange(0.05, 0.55, 0.05):
            self.vid_classes[t] = {}
            for l in [VIRAL, QUALITY, JUNK]:
                self.vid_classes[t][l] = 0
        
        #File with the class of each video
        summary_file = os.path.join(self.outf, 'pnas_classes.txt')
        self.classes_file = open(summary_file, 'w')
                
    def _reduce(self, key, value):
        self.total += 1
        if value[0] != UNKNOWN:
            self.known += 1
            if value[0] != MEMORYLESS:
                for threshold in np.arange(0.05, 0.55, 0.05):
                    cls = value[threshold]
                    self.vid_classes[threshold][value[threshold]] += 1
                    print(key, NAMES[cls], threshold, file=self.classes_file)
            else:
                print(key, NAMES[MEMORYLESS], file=self.classes_file)

    def close(self):
        self.classes_file.close()
        
        peak_bar_total = []
        slow_bar_total = []
        fast_bar_total = [] 
        
        peak_bar_know = []
        slow_bar_know = []
        fast_bar_know = []
        
        results_fpath = os.path.join(self.outf, 
                                    'video_types_varyingt-'+self.append+'.txt')
        with open(results_fpath, 'w') as results_file:
            for t in np.arange(0.05, 0.55, 0.05):
                print('t = %.2f: Viral < t; t < Quality < 1 - t; Junk > t' %t,
                      file=results_file)
                vid_classes = self.vid_classes[t]
                if self.known > 0:
                    for cls in [VIRAL, QUALITY, JUNK]:
                        frac_total = vid_classes[cls] / self.total
                        frac_know = vid_classes[cls] / self.known
                        
                        if cls == VIRAL:
                            peak_bar_total.append(frac_total)
                            peak_bar_know.append(frac_know)
                        elif cls == QUALITY:
                            slow_bar_total.append(frac_total)
                            slow_bar_know.append(frac_know)
                        else:
                            fast_bar_total.append(frac_total)
                            fast_bar_know.append(frac_know)
                        
                        print('Type = %s; FracTotal = %.5f, FracKnown = %.5f' 
                              %(cls, frac_total, frac_know), file=results_file)
                    print('---', file=results_file)
                    frac = self.known / self.total
                    print('Total = %d; Known = %.5f'%(self.total, frac),
                          file=results_file)
                print(file=results_file)
                print('==================', file=results_file)

            barplot(Bars(peak_bar_total, name='Viral'), 
                    Bars(slow_bar_total, name='Quality'), 
                    Bars(fast_bar_total, name='Junk'),
                    xticklabels=np.arange(5, 55, 5),
                    xlabel=r'Class Threshold - t (\%)',
                    ylabel='Fraction of Videos',
                    ymin=0, ymax=1, grid=False,
                    outputf=os.path.join(self.outf, 
                                         'video_types_total_plot-'+
                                         self.append+'.png'))

            barplot(Bars(peak_bar_know, name='Viral'), 
                    Bars(slow_bar_know, name='Quality'), 
                    Bars(fast_bar_know, name='Junk'), 
                    xticklabels=np.arange(5, 55, 5),
                    xlabel=r'Class Threshold - t (\%)',
                    ylabel='Fraction of Videos',
                    ymin=0, ymax=1, grid=False,
                    outputf=os.path.join(self.outf, 
                                         'video_types_known_plot-'+
                                         self.append+'.png'))

class PNAS(YoutimeH5Runner):

    def __init__(self, name, description):
        super(PNAS, self).__init__(name, description)
        self.mapper_obj = None
        self.reducer_obj = None
    
    def item_generator(self):
        return self.igen
    
    def mapper(self):
        return self.mapper_obj
    
    def reducer(self):
        return self.reducer_obj
    
    def add_custom_aguments(self, parser):
        super(PNAS, self).add_custom_aguments(parser)
        parser.add_argument("days", type=int,
                            help="How many days in each window")
    
    def finalize(self):
        self.reducer_obj.close()
    
    def setup(self, arg_vals):
        self.igen = PyTablesDaoIterator(arg_vals.in_file, arg_vals.table)
        append = str(arg_vals.days)
        self.mapper_obj = PNASMapper(arg_vals.days)
        self.reducer_obj = PNASReducer(arg_vals.outf, append)
        
if __name__ == '__main__':
    runner = PNAS(sys.argv[0], __doc__)
    runner(sys.argv[1:])