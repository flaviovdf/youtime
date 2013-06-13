# -*- coding: utf8
'''
For each collection of video duplicates, this script will scatter plot
difference from the total views most popular video to the mean/median
of views all the others, with the pairwise cosine and mean/median cosine 
between the events most viewed video of a duplicate group and the events of
all of its duplicates. 
'''
from __future__ import division, print_function

from collections import defaultdict
from scipy import spatial

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

class DuplicateEventsMapper(BaseMapper):
    
    def __init__(self):
        groups = VideoDAO.EV_GROUPS
        del groups['NOT_CAPTURED']
        
        self.group_to_int = {}
        for gid, group in enumerate(groups):
            self.group_to_int[group] = gid
    
    def _map(self, key, video_group):
        #Finding maximum
        winner_id = None
        max_views = 0
        for video_id, video_data in video_group.iteritems():
            total_view = video_data[1]
            if total_view > max_views:
                winner_id = video_id
                max_views = total_view
        
        #Convert data to arrays
        num_groups = len(self.group_to_int)
        num_videos = len(video_group)
        events_array = np.ndarray((num_videos - 1, num_groups))
        winner_array = np.ndarray(num_groups)
        views_array = np.ndarray(num_videos - 1)
        
        i = 0
        for video_id, video_data in video_group.iteritems():
            event_views = video_data[0]
            total_views = video_data[1]
            
            temp_array = np.zeros(num_groups)
            for event_group, views in event_views:
                gid = self.group_to_int[event_group]
                temp_array[gid] = views
            
            if video_id == winner_id:
                winner_array = temp_array
            else:
                views_array[i] = total_views
                events_array[i] = temp_array
                i += 1
        
        #Compute differences and cosines
        total_differences = max_views - views_array
        event_aggr_differences = np.ndarray(num_videos - 1)
        cosines = np.ndarray(num_videos - 1)
        for i in xrange(num_videos - 1):
            event_array = events_array[i]
            event_diff = winner_array - event_array
            event_aggr_differences[i] = np.mean(event_diff)
            
            scaled_winner = \
                (winner_array - np.mean(winner_array)) / np.std(winner_array)
            scaled_events = \
                (event_array - np.mean(event_array)) / np.std(event_array)
            cos = spatial.distance.cosine(scaled_winner, scaled_events)
            cosines[i] = 1.0 - cos
        
        #Grouped differences
        grouped_differences = np.ndarray((num_groups, num_videos - 1))
        for group in self.group_to_int:
            gid = self.group_to_int[group]
            diff = winner_array[gid] - events_array[:,gid]
            grouped_differences[gid] = diff
            
        return max_views, total_differences, cosines, event_aggr_differences, \
            grouped_differences, self.group_to_int
    
class DuplicateEventsReducer(BaseReducer):
        
    def __init__(self, outf):
        self.outf = outf
        
        self.avgmedian_x = []
        self.avg_y_tot_diff = []
        self.median_y_tot_diff = []
        self.avg_y_event_diff = []
        self.median_y_event_diff = []
        self.avg_y_cos = []
        self.median_y_cos = []   
    
        self.pairwise_x = []
        self.pairwise_y_tot_diff = []
        self.pairwise_y_event_diff = []
        self.pairwise_y_cos = []
        
        self.group_pairwise = defaultdict(list)

    def _reduce(self, key, value):
        max_views, total_differences, cosines, event_aggr_differences, \
            grouped_differences, group_to_int = value
        
        self.avgmedian_x.append(max_views)
        
        self.avg_y_tot_diff.append(np.mean(total_differences))
        self.median_y_tot_diff.append(np.median(total_differences))
        
        self.avg_y_event_diff.append(np.mean(event_aggr_differences))
        self.median_y_event_diff.append(np.median(event_aggr_differences))
        
        self.avg_y_cos.append(np.mean(cosines))
        self.median_y_cos.append(np.median(cosines))
        
        for i in xrange(len(total_differences)):
            self.pairwise_x.append(max_views)
            self.pairwise_y_tot_diff.append(total_differences[i])
            self.pairwise_y_event_diff.append(event_aggr_differences[i])
            self.pairwise_y_cos.append(cosines[i])
        
        #Per Group
        for group in group_to_int:
            gid = group_to_int[group]
            self.group_pairwise[group].extend(grouped_differences[gid])
        
    def close(self):
        #Plotting mean/median total differences
        scatter_avg_tot_diff = XYPoints(self.avgmedian_x, self.avg_y_tot_diff,
                                        style='bo')
        scatter_median_tot_diff = XYPoints(self.avgmedian_x, 
                                           self.median_y_tot_diff,
                                           style='bo')
        xy_plot(scatter_avg_tot_diff, 
                xlabel='Number of Final Views', 
                ylabel='Mean Total Difference', 
                outputf=os.path.join(self.outf, 'dups-mean-tot-diff.png'),
                logx=True, logy=True)
        xy_plot(scatter_median_tot_diff, 
                xlabel='Number of Final Views', 
                ylabel='Median Total Difference', 
                outputf=os.path.join(self.outf, 'dups-median-tot-diff.png'),
                logx=True, logy=True)

        #Plotting mean/median cosines                
        scatter_median_cos = XYPoints(self.avgmedian_x, self.median_y_cos,
                                         style='bo')
        scatter_avg_cos = XYPoints(self.avgmedian_x, self.avg_y_cos,
                                   style='bo')

        xy_plot(scatter_avg_cos, 
                xlabel='Number of Final Views', 
                ylabel='Mean (1 - Cosine)', 
                outputf=os.path.join(self.outf, 'dups-mean-cos.png'),
                logx=True, logy=True)
        xy_plot(scatter_median_cos, 
                xlabel='Number of Final Views', 
                ylabel='Median (1 - Cosine)', 
                outputf=os.path.join(self.outf, 'dups-median-cos.png'),
                logx=True, logy=True)

        #Plotting mean/median event differences                
        scatter_median_event_diff = XYPoints(self.avgmedian_x, 
                                             self.median_y_event_diff,
                                             style='bo')
        scatter_avg_event_diff = XYPoints(self.avgmedian_x, 
                                          self.avg_y_event_diff,
                                          style='bo')

        xy_plot(scatter_avg_event_diff, 
                xlabel='Number of Final Views', 
                ylabel='Mean Aggregate Event Difference', 
                outputf=os.path.join(self.outf, 'dups-mean-event-diff.png'),
                logx=True, logy=True)
        xy_plot(scatter_median_event_diff, 
                xlabel='Number of Final Views', 
                ylabel='Median Aggregate Event Difference', 
                outputf=os.path.join(self.outf, 'dups-median-event-diff.png'),
                logx=True, logy=True)
        
        #Pairwise plots
        scatter_pairwise_tot_diff = XYPoints(self.pairwise_x, 
                                             self.pairwise_y_tot_diff,
                                             style='bo')
        scatter_pairwise_event_diff = XYPoints(self.pairwise_x, 
                                               self.pairwise_y_event_diff,
                                               style='bo')
        scatter_pairwise_cos = XYPoints(self.pairwise_x, self.pairwise_y_cos,
                                        style='bo')
        xy_plot(scatter_pairwise_tot_diff, 
                xlabel='Number of Final Views', 
                ylabel='Total Difference', 
                outputf=os.path.join(self.outf, 'dups-pairwise-tot-diff.png'),
                logx=True, logy=True)
        xy_plot(scatter_pairwise_event_diff, 
                xlabel='Number of Final Views', 
                ylabel='Aggregate Event Difference', 
                outputf=os.path.join(self.outf, 'dups-pairwise-event-diff.png'),
                logx=True, logy=True)
        xy_plot(scatter_pairwise_cos, 
                xlabel='Number of Final Views', 
                ylabel='1 - Cosine', 
                outputf=os.path.join(self.outf, 'dups-paiwise-cos.png'),
                logx=True, logy=True)
        
        
        #Group plot
        lines = []
        style = VideoDAO.GROUP2STYLE
        for group in sorted(self.group_pairwise,
                            key=lambda g: np.mean(self.group_pairwise[g]),
                            reverse=True):
            group_cdf = ecdf(self.group_pairwise[group])
            #1 minus for ccdf
            line = XYPoints(group_cdf[0], 1 - group_cdf[1], group, style[group])
            fpath = os.path.join(self.outf, 'dups-diff-%s.dat'%group)
            write_stats_to_file(self.group_pairwise[group], fpath)
            lines.append(line)
        
        xy_plot(*lines,
                  xlabel='Views Difference', 
                  ylabel='Prob. (Views Difference > x)',
                  grid=False,
                  legborder=False,
                  logx=True, logy=False,
                  outputf=os.path.join(self.outf, 'dups-diff-grouped.png'))
        
        #CDF of mean cosine
        coscdf_cdf = ecdf(self.avg_y_cos)
        line = XYPoints(coscdf_cdf[0], 1 - coscdf_cdf[1], 
                        'CCDF Avg. Cosine', 'bo')
        xy_plot(line,
                  xlabel='Avg. Cosine', 
                  ylabel='Prob. (Avg. Cosine > x)',
                  grid=False,
                  legborder=False,
                  logx=True, logy=False,
                  outputf=os.path.join(self.outf, 'dups-ccdf-cos.png'))
                
        #CDF of aggregated
        aggcdf_cdf = ecdf(self.avg_y_event_diff)
        line = XYPoints(aggcdf_cdf[0], 1 - aggcdf_cdf[1], 
                        'Aggregate Event Difference', 'bo')
        xy_plot(line,
                  xlabel='CCDF Aggregate Event Difference', 
                  ylabel='Prob. (Avg. Agg. Diff > x)',
                  grid=False,
                  legborder=False,
                  logx=True, logy=False,
                  outputf=os.path.join(self.outf, 'dups-ccdf-agg.png'))
                
class DuplicateEventsRunner(YoutimeH5Runner):

    def __init__(self, name, description):
        super(DuplicateEventsRunner, self).__init__(name, description)
        self.mapper_obj = None
        self.reducer_obj = None
    
    def item_generator(self):
        return self.igen
    
    def mapper(self):
        return self.mapper_obj
    
    def reducer(self):
        return self.reducer_obj
    
    def add_custom_aguments(self, parser):
        super(DuplicateEventsRunner, self).add_custom_aguments(parser)
        parser.add_argument('duplicates_fpath', type=str, 
                            help='Duplicates file')
        parser.add_argument('--min_vids', type=int, default=1,
                            help='Minimum of videos per group')
    def setup(self, arg_vals):
        self.mapper_obj = DuplicateEventsMapper()
        self.reducer_obj = DuplicateEventsReducer(arg_vals.outf)
        
        duplicates_fpath = arg_vals.duplicates_fpath
        self.igen = DuplicateGroupsIgen(arg_vals.in_file, duplicates_fpath, 
                                        arg_vals.table, arg_vals.min_vids)
        
    def finalize(self):
        self.reducer_obj.close()
        
if __name__ == '__main__':
    runner = DuplicateEventsRunner(sys.argv[0], __doc__)
    runner(sys.argv[1:])