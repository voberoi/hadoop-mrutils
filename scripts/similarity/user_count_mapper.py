#!/usr/bin/env python
# encoding: utf-8
"""
user_count_mapper.py

Compute the count of values for each user (ratings, playcounts, etc). 

Run from the Amazon Elastic MapReduce console with the following 
parameters:

-input elasticmapreduce/samples/similarity/lastfm/input 
-output <yourbucket>/lastfm/output/user-counts 
-mapper elasticmapreduce/samples/similarity/user_count_mapper.py 
-reducer aggregate


Created by Peter Skomoroch on 2009-03-30.
Copyright (c) 2009 Data Wrangling LLC. All rights reserved.
http://www.datawrangling.com/
"""

import sys

for line in sys.stdin:
  (user, item, rating) = line.strip().split()
  print "LongValueSum:%s\t1" % user

