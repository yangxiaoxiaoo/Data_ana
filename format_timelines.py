#!/usr/bin/python
#AUg20 read friends timeline and put them in to username files
#in all context fid means friend id, not follewer...follower = user
import os
import sys
import ast
import json, datetime, calendar
import pickle
import subprocess
list_infile = '/net/data/twitter-viewing/crawl/dist_crawler_clear/crawler/new_friendids_parsed.txt'
open_limit = 1020


def reverse_dict(user_to_friends):
    reversed = dict()
    for id_set in user_to_friends.itervalues():
        for id in id_set:
            reversed[id] = set()
    for key, value in user_to_friends.iteritems():
        for fids in value:
            reversed[fids].add(key)
    return reversed

def read_list():
    dict_original = dict()
    fp = open(list_infile, 'r')
    for line in fp:
        try:
            uid = line.split(' [')[0]
            friends_list = line.split(' [')[1].strip(']').split(',')
            friends_set = set()
            for ele in friends_list:
                friends_set.add(ele.strip(' '))
            dict_original[uid] = friends_set
        except Exception as Enptylist:
           # print "Enpty Friendlist!"
            pass
    print len(dict_original)
    return dict_original

def chunk_set(i):
    uid_set = set()
    fp = open(list_infile, 'r')
    j = 0
    for line in fp:
        j += 1
        if open_limit*i < j <= open_limit*(i+1):
            try:
                uid = line.split(' [')[0]
                uid_set.add(uid)
            except Exception as err_EOF:
                pass
    return uid_set

def save_finish_log(i, pool_of_users):
    #logfile--number of iteration, number of finished users, list of finished users
    fp = open("log.txt", 'a')
    fp.write(str(i) + "::" + str(len(pool_of_users))+ "::" + str(pool_of_users))

def save_error_log(string):
    fp = open("errors.txt", 'a')
    fp.write(string + '\n')

def string_to_unixtime(time_raw):
    dt = datetime.datetime.strptime(time_raw, '%a %b %d %H:%M:%S +0000 %Y')
    unix_dt = calendar.timegm(dt.utctimetuple())
    return unix_dt



def steam_file(pool_of_users, friends_to_user, indir, outdir):
    for data_files in os.listdir(indir):
        print(data_files)
        data_fp = open(os.path.join(indir, data_files), 'r')
        file_dict = dict()
        for line in data_fp:
            try:
                json_str = line.split(";;")[1]
                obj = json.loads(json_str)
                uid = line.split(';;')[0]

                try:
                    for interested_user in friends_to_user[uid]:
                        if interested_user in pool_of_users:#set
                            file_dict[interested_user] = open(os.path.join(outdir, interested_user), 'a')
                            file_dict[interested_user].write(uid +'::'+ str(obj["id"]) +'::' + str(string_to_unixtime(obj["created_at"])) +"::" + str(obj["retweeted"]) + '::'+ str(obj["source"])+ "\n")
                except Exception as key_not_exist:
                    save_error_log("changed_friends::" + uid)
            except Exception as Json_empty:
                save_error_log("empty_json::friend_data_file = " +data_files+'::' + line)
                pass




def main(indir, outdir, i):
    user_to_friends = read_list()
    friends_to_user = reverse_dict(user_to_friends)
    print "dictionary formed:"
  #  print friends_to_user
  #  for iteration in range(0, 9):
    machine_index = int(i)
    if 0<=  machine_index < 10:
        out_users = chunk_set(machine_index)
        steam_file(out_users, friends_to_user, indir, outdir)
        #save context in case of exception, in logfile
        save_finish_log(machine_index, out_users)
        print "machine job done:" + str(machine_index)
    else:
        print("error: index should be 0 to 9")




if __name__ == "__main__":
    #argv[1] infile argv[2] outfile
    #[3] useri
    main(sys.argv[1], sys.argv[2], sys.argv[3])