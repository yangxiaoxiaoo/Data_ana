#!/usr/bin/python
import os
import collections

def sort_lines(line_dict, collision_lines, file_username):
    outfiles = '/net/data/twitter-viewing/crawl/dist_crawler_clear/crawler/friends_timeline_unmarked/'
    outfp = open(os.path.join(outfiles, file_username), 'a')
    order_lines = collections.OrderedDict(sorted(line_dict.items()))
    if len(collision_lines) == 0:
        for key, value in order_lines.iteritems():
            outfp.write(value)
    else:
        for key, value in order_lines.iteritems():
            #since the collistion is very small it's ok to for loop it hopefully
            for coll_line in collision_lines:
                coll_timestamp = coll_line.split('::')[2]
                if key == coll_timestamp:
                    outfp.write(coll_line)
            outfp.write(value)
    outfp.close()




def main():
    infiles = '/net/data/twitter-viewing/crawl/dist_crawler_clear/crawler/friends_timeline_arr/'
    for file in os.listdir(infiles):
        line_dict = dict()
        time_set = set()
        collision_lines = set()
        fp = open(os.path.join(infiles, file), 'r')
        for line in fp:
            timestamp = line.split('::')[2]
            #if there is duplicate in timestamps, write it down and insert it later to the same place in the dictionary
            if timestamp in time_set:
                collision_lines.add(line)
            else:
                time_set.add(timestamp)
            line_dict[timestamp] = line
        fp.close()
        sort_lines(line_dict, collision_lines, file)

if __name__ is "__main__":
    main()