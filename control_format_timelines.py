#!/usr/bin/python
import sys, os, subprocess, math

machines = ["achtung%02i" % (x) for x in range (2, 12)]
procs = []

for i, machine in enumerate(machines):
    print "i = "
    print i
    cmd = ['ssh',
           machine,
           '/net/data/twitter-viewing/crawl/dist_crawler_clear/crawler/Data_ana/format_timelines.py',
           '/net/data/twitter-viewing/crawl/dist_crawler_clear/crawler/friends_timeline/',
           '/net/data/twitter-viewing/crawl/dist_crawler_clear/crawler/friends_timeline_arr/',
           '%i' % (i)
           ]
    print cmd
    procs.append(subprocess.Popen(cmd))

for proc in procs:
    proc.wait()