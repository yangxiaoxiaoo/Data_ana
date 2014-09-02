#!/usr/bin/python
import os
import collections

def sort_lines(line_dict,  file_username):
    print "sort_lines"
    outfiles = '/net/data/twitter-viewing/crawl/dist_crawler_clear/crawler/friends_timeline_unmarked/'
    outfp = open(os.path.join(outfiles, file_username), 'a')
    order_lines = collections.OrderedDict(sorted(line_dict.items()))
    for key, value in order_lines.iteritems():
        outfp.write(value)
    outfp.close()



def main():
    infiles = '/net/data/twitter-viewing/crawl/dist_crawler_clear/crawler/friends_timeline_arr/'
  #  print infiles
    for file in os.listdir(infiles):
        line_dict = dict()
        fp = open(os.path.join(infiles, file), 'r')
        for line in fp:
         #   print line
            try:
                timestamp = line.split('::')[2]
                #if there is duplicate in timestamps, write it down and insert it later to the same place in the dictionary
                line_dict[timestamp] = line
            except Exception as EOF:
                pass
        fp.close()
        sort_lines(line_dict,  file)

if __name__ == "__main__":
    main()