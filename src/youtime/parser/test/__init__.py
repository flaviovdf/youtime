# -*- coding: utf8
'''
Tests for html and xml parsing.
'''
from __future__ import division, print_function

import os

DATA_DIR = os.path.join(__path__[0], 'data')

#ENG FILES
ENG_DATA_DIR = os.path.join(DATA_DIR, 'eng')

eng_info_fname = 'videoinfo-fhGb6qPiluE14-Apr-2010_13-14-31'
ENG_INFO_FILE = os.path.join(ENG_DATA_DIR, eng_info_fname)

eng_info_fname = 'videoinfo-5PsnxDQvQpw06-Apr-2010_2-10-21'
ENG_INFO_FILE2 = os.path.join(ENG_DATA_DIR, eng_info_fname)

eng_stats_fname = 'videostats-fhGb6qPiluE14-Apr-2010_13-14-31'
ENG_STATS_FILE = os.path.join(ENG_DATA_DIR, eng_stats_fname)

#PTBR FILES
PTBR_DATA_DIR = os.path.join(DATA_DIR, 'ptbr')

ptbr_info_fname = 'videoinfo-10-26-10-2009-12-7-41'
PTBR_INFO_FILE = os.path.join(PTBR_DATA_DIR, ptbr_info_fname)

ptbr_stats_fname = 'videostat-10-26-10-2009-12-7-41'
PTBR_STATS_FILE = os.path.join(PTBR_DATA_DIR, ptbr_stats_fname)

#NEW FILES
NEW_DATA_DIR = os.path.join(DATA_DIR, 'newfmt012')

new_info_fname = 'videoinfo-p0Jwx7n2cwc16-Feb-2012_10-14-29'
NEW_INFO_FILE = os.path.join(NEW_DATA_DIR, new_info_fname)

new_info_fname = 'videoinfo-ZzmEBY6lVAE16-Feb-2012_10-27-33'
NEW_INFO_FILE2 = os.path.join(NEW_DATA_DIR, new_info_fname)

new_stats_fname = 'videostats-p0Jwx7n2cwc16-Feb-2012_10-14-29'
NEW_STATS_FILE = os.path.join(NEW_DATA_DIR, new_stats_fname)

new_stats_fname = 'videostats-ZzmEBY6lVAE16-Feb-2012_10-27-33'
NEW_STATS_FILE2 = os.path.join(NEW_DATA_DIR, new_stats_fname)

#TOPIC
TOPIC_DATA_DIR = os.path.join(DATA_DIR, 'topic')

topic_stats_fname = '1371083611594-zgY-cR0kv5Y-stats.html'
TOPIC_STATS_FILE = os.path.join(TOPIC_DATA_DIR, topic_stats_fname)

topic_stats_fname2 = '1371083001541-G-zjxv6hL1o-stats.html'
TOPIC_STATS_FILE2 = os.path.join(TOPIC_DATA_DIR, topic_stats_fname2)

topic_stats_fname3 = '1371076437632-GYmbNzNHtgE-stats.html'
TOPIC_STATS_FILE3 = os.path.join(TOPIC_DATA_DIR, topic_stats_fname3)

topic_stats_fname4 = '1371079085840-9Rp33PvBSns-stats.html'
TOPIC_STATS_FILE4 = os.path.join(TOPIC_DATA_DIR, topic_stats_fname4)

topic_stats_fname5 = '1371076440111-Qa868FI0h3o-stats.html'
TOPIC_STATS_FILE5 = os.path.join(TOPIC_DATA_DIR, topic_stats_fname5)

topic_stats_fname6 = '1371078677568-X2B6-44q91Y-stats.html'
TOPIC_STATS_FILE6 = os.path.join(TOPIC_DATA_DIR, topic_stats_fname6)

topic_info_fname = '1371083611601-zgY-cR0kv5Y-content.html'
TOPIC_INFO_FILE = os.path.join(TOPIC_DATA_DIR, topic_info_fname)