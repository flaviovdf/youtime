# -*- coding: utf8

from __future__ import division, print_function

from collections import defaultdict
from scipy import stats

from vod.fileutil import write_stats_to_file
from vod.stats.curves import ecdf
from vod.mapreducescript import BaseMapper
from vod.mapreducescript import BaseReducer
from vodstats.plot.plot2d import XYPoints, xy_plot

from youtime import YoutimeH5Runner
from youtime.common.dao import VideoDAO
from youtime.mapred.ig import DuplicateGroupsIgen

import numpy as np
import os
import sys

class DuplicatesRankCorrMapper(BaseMapper):
    
    def __init__(self):
        groups = VideoDAO.EV_GROUPS.copy()
        del groups['NOT_CAPTURED']
        
        self.ref_group_to_int = {}
        for gid, group in enumerate(groups):
            self.ref_group_to_int[group] = gid
            
    def _map(self, key, video_group):
        
        #Creates a data matrix with the number of views per video (rows) for
        #each referrer (columns).
        i = 0
        n_rows = len(video_group)
        n_cols = len(self.ref_group_to_int) + 1
        data = np.zeros(shape=(n_rows, n_cols))
        for video_data in video_group.values():
            referral_views = video_data[0]
            total_view = video_data[1]
            
            for ref_group, ref_views in referral_views:
                ref_group_id = self.ref_group_to_int[ref_group]
                data[i][ref_group_id] = ref_views
            
            #Last column has total views
            data[i][-1] = total_view;
            i += 1
    
        #Generating correlations
        total_view_array = data[:,-1]
        return_val = {}
        for ref_group, ref_group_id in self.ref_group_to_int.iteritems():
            ref_group_array = data[:,ref_group_id]
            k_tau = stats.kendalltau(total_view_array, ref_group_array)
            s_rho = stats.spearmanr(total_view_array, ref_group_array)
            return_val[ref_group] = (k_tau, s_rho)
        
        return return_val
    
class DuplicatesRankCorrReducer(BaseReducer):
    
    def __init__(self, out_folder_path):
        self.to_plot_ktau = defaultdict(list)
        self.to_plot_srho = defaultdict(list)
        self.out_folder_path = out_folder_path
    
    def _reduce(self, key, value):
        for ref_group, corr in value.iteritems():
            k_tau, s_rho = corr
            if not np.isnan(k_tau[0]) and k_tau[1] <= 0.05:
                self.to_plot_ktau[ref_group].append(k_tau)
                
            if not np.isnan(s_rho[0]) and s_rho[1] <= 0.05:
                self.to_plot_srho[ref_group].append(s_rho)
                
    def close(self):
        style = VideoDAO.GROUP2STYLE
        
        lines_ktau = []
        for group in sorted(self.to_plot_ktau, 
                            key=lambda g: np.mean(self.to_plot_ktau[g]),
                            reverse=True):
            group_cdf = ecdf(self.to_plot_ktau[group])
            #1 minus for ccdf
            line = XYPoints(group_cdf[0], 1 - group_cdf[1], group, style[group])
            lines_ktau.append(line)
            fpath = os.path.join(self.out_folder_path, 'dups-ktau-%s.dat'%group)
            write_stats_to_file(self.to_plot_ktau[group], fpath)
            
        xy_plot(*lines_ktau,
              xlabel='Kendall Tau', 
              ylabel='Prob. (Kendall Tau > x)',
              grid=False,
              legborder=False,
              outputf=os.path.join(self.out_folder_path, 'dups-ktau.png'))

        lines_srho = []
        for group in sorted(self.to_plot_srho, 
                            key=lambda g: np.mean(self.to_plot_srho[g]),
                            reverse=True):
            group_cdf = ecdf(self.to_plot_srho[group])
            #1 minus for ccdf
            line = XYPoints(group_cdf[0], 1 - group_cdf[1], group, style[group])
            lines_srho.append(line)
            fpath = os.path.join(self.out_folder_path, 'dups-srho-%s.dat'%group)
            write_stats_to_file(self.to_plot_srho[group], fpath)

        xy_plot(*lines_srho,
              xlabel='Spearman Rho', 
              ylabel='Prob. (Spearman Rho > x)',
              grid=False,
              legborder=False,
              outputf=os.path.join(self.out_folder_path, 'dups-srho.png'))
            
class DuplicatesRankCorrRunner(YoutimeH5Runner):

    def __init__(self, name, description):
        super(DuplicatesRankCorrRunner, self).__init__(name, description)
        self.mapper_obj = None
        self.reducer_obj = None
    
    def item_generator(self):
        return self.igen
    
    def mapper(self):
        return self.mapper_obj
    
    def reducer(self):
        return self.reducer_obj
    
    def add_custom_aguments(self, parser):
        super(DuplicatesRankCorrRunner, self).add_custom_aguments(parser)
        parser.add_argument('duplicates_fpath', type=str, 
                            help='Duplicates file')
        parser.add_argument('--min_vids', type=int, default=1,
                            help='Minimum of videos per group')
                
    def setup(self, arg_vals):
        self.mapper_obj = DuplicatesRankCorrMapper()
        self.reducer_obj = DuplicatesRankCorrReducer(arg_vals.outf)
        
        duplicates_fpath = arg_vals.duplicates_fpath
        self.igen = DuplicateGroupsIgen(arg_vals.in_file, duplicates_fpath, 
                                        arg_vals.table, arg_vals.min_vids)
        
    def finalize(self):
        self.reducer_obj.close()
        
if __name__ == '__main__':
    runner = DuplicatesRankCorrRunner(sys.argv[0], __doc__)
    runner(sys.argv[1:])
