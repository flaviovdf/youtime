#!/usr/bin/env python
# -*- coding: utf8

'''
This script serves as an example on how to read CSV data published on http://vod.dcc.ufmg.br/traces/youtime
'''

from time import localtime

import csv
import sys

def convert_to_array(s, dtype=str):
    '''
    Converts a string in form format "[data0, data1, data2, ... ]" to a real array.

    Arguments:
    s -- the string

    Keywork arguments:
    dtype=str -- A basic python type (str, int flaot) to convert each value of the array to.
    '''
    return [dtype(d.strip()) for d in s[1:-1].split(',')] if len(s) > 2 else []

def main(args=None):
    if not args: args = []

    if len(args) != 2:
        print >>sys.stderr, 'Usage %s <csv file>'%args[0]
        return 2

    fpath = args[1]
    with open(fpath) as f:
        dictr = csv.DictReader(f)

        for data in dictr:
            print data['#ID']
            print '\t', convert_to_array(data['VIEW_DATA'], float) # Basic converstion to floats
            print '\t', convert_to_array(data['EVENT_TYPES'])
            print '\t', convert_to_array(data['EVENT_DATES'], lambda d: localtime(float(d))) #Convert to date

if __name__ == '__main__':
    sys.exit(main(sys.argv))
