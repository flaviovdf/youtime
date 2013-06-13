# -*-: coding: utf8
'''
Package for common code.
'''

from __future__ import division, print_function

import sys

DEBUG=True
def log(msg):
    if DEBUG:
        print(msg, file=sys.stderr)