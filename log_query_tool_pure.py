#!/usr/bin/python
# -*- coding: utf-8 -*-
import pymongo, json
import time
import sys
from bson.objectid import ObjectId
#from pymongo.read_preferences import ReadPreference
from pymongo import ReadPreference
from optparse import OptionParser

base = [str(x) for x in range(10)] + [ chr(x) for x in range(ord('a'),ord('a')+6)]

mongo_conn = {
    "conn": "mongodb://chivoxapi:cs!2015@10.0.200.19:27017", # 国科机房-slave
    "read_preference": ReadPreference.SECONDARY
}

def dec2hex(string_num):
    num = int(string_num)
    mid = []
    while True:
        if num == 0: break
        num,rem = divmod(num, 16)
        mid.append(base[rem])
    return ''.join([str(x) for x in mid[::-1]])

def get_id_from_date(date_str):
    t_str = time.strptime(date_str, '%Y-%m-%d %X')
    t_unix = str(int(time.mktime(t_str)))
    t_id = dec2hex(t_unix) + '0000000000000000'
    return t_id

def connnet_mongo():
    m = pymongo.MongoClient(mongo_conn['conn'], read_preference=mongo_conn['read_preference'])
    return m

def log_query(m,db,coll,q,**args):
    if 'ff' in args.keys() and args['ff'] == True:
        docs = m[db][coll].find(q)
        for doc in docs:
            doc['_id'] = str(doc['_id'])
            print json.dumps(doc)
    else:
        print "number:" + str(m[db][coll].find(q).count())
    m.close()

def main():

    MSG_USAGE = 'python new.py -t <time param> -q <query param> -o <True or False>'
    parser = OptionParser(MSG_USAGE)
    parser.add_option('-t', '--time', dest='tRange', default="",
                      help="""required,example: -t '["2015-06-01 00:00:00", "2015-06-02 00:00:00"]'""")
    parser.add_option('-q', '--query', dest='search', default="",
                      help="""used to specify query in log collection,example:'{"est":12}'""")
    parser.add_option('-o', '--out', dest='outFlag', default="False",
                      help="""used to specify whether use find(True) or count(False)""")
    (options, args) = parser.parse_args()
    tRange = options.tRange
    log = options.search
    ff = options.outFlag
    if tRange == "":
        print '-t params is must!'
        parser.print_help()
        sys.exit()
    tRange = json.loads(tRange)
    y = tRange[0][:4]
    m = tRange[0][5:7]
    if int(m)%2 == 1:
        if int(m) < 9:
            m = '0' + str(int(m)+1)
        else:
            m = str(int(m)+1)
    db = y + '_' + m
    try:
        start_id = ObjectId(get_id_from_date(tRange[0]))
        end_id = ObjectId(get_id_from_date(tRange[1]))
    except ValueError:
        print '-t params format invalid!'
        parser.print_help()
        sys.exit()
    if log != "":
        m = connnet_mongo()
        coll = 'log'
        log = json.loads(log)
        query = {"_id": {"$gte": start_id, "$lte":end_id}}
        query.update(log)
        if ff == "False":
            log_query(m,db,coll,query)
        else:
            log_query(m,db,coll,query,ff=True)

if __name__ == '__main__':
    main()
