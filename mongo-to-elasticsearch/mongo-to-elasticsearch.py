'''
Tested with:
Mongo 3.0.12
pymongo 3.3.0

Elasticsearch 2.1.2
Kibana 4.3.3
elasticsearch python 2.1.0
'''

import elasticsearch
# import pprint

from pymongo import MongoClient
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, parallel_bulk
from collections import deque
from pprint import pprint
from tqdm import tqdm
from time import time, sleep

from lib_time import datetime_from_timestamp, datetime_from_str
from lib_time import datetime_to_timestamp, datetime_to_str
from lib_time import sleep_seconds

# client = MongoClient("mongodb://188.166.40.27:27017/")
# db = client['twixplorer']
# tweets = db['tweets']

# Define vars
esclient = Elasticsearch(['http://localhost:9200'])
mgclient = MongoClient('mongodb://hash-api-js:p4vK8tKWPoEN19QNhu2527BvuM6TUiKEpzAo@35.195.195.26:27017/inep')

DATABASE = 'inep'
# This will be the index you need to select in Kibana
# it is also the database to use in mongo to pull data from

db = mgclient[DATABASE]
col = db['tweets']

# get start timestamp
timestamp_now = int(time())
datetime_now = datetime_from_timestamp(timestamp_now, utc=True)
datetime_now = datetime_to_str(datetime_now, '%d-%m-%Y')
datetime_now = datetime_from_str(datetime_now, '%d-%m-%Y')
timestamp_now = datetime_to_timestamp(datetime_now)

# get finish timestamp
timestamp_end = timestamp_now - (7*3600*24)
datetime_end = datetime_from_timestamp(timestamp_end, utc=True)
datetime_end = datetime_to_str(datetime_end, '%d-%m-%Y')
datetime_end = datetime_from_str(datetime_end, '%d-%m-%Y')
timestamp_end = datetime_to_timestamp(datetime_end)

# convert to miliseconds
time_zone = 7200*1000
timestamp_now = (timestamp_now * 1000) + time_zone
timestamp_end = (timestamp_end * 1000) + time_zone

# set to query tweets from now
timestamp_query = timestamp_now

# pull from mongo and dump into ES using bulk API
actions = []; actions_append = actions.append

for i in range(1,15):
    title = 'twitter-'+datetime_to_str(datetime_from_timestamp(timestamp_query/1000),'%Y-%m-%d')
    print(str(i)+'/14:', datetime_from_timestamp(timestamp_query/1000, '%d-%m-%Y'), '->', title)
    queryStart = {'status.timestamp_ms': {"$gte": timestamp_query-(12*3600*1000)}}
    queryEnd = {'status.timestamp_ms': {"$lte": timestamp_query}}
    col_find = col.find({"$and":[queryStart,queryEnd]})
    col_total = col_find.count()

    for data in tqdm(col_find, total=col_total):
        # fixes from Mongo to Elasticsearch
        # data.pop('_id')
        data = dict(data)
        data['status']['timestamp_ms'] = int(data['status']['timestamp_ms'])
        geocode = data['reverse_geocode']

        if isinstance(geocode,list):
            temp = geocode[0]
            geocode[0]=geocode[1]
            geocode[1]=temp
            data['status']['reverse_geocode']=geocode
        else: data['status']['reverse_geocode']=None

        action = {
            "_index": title,
            "_type": "sslog",
            "_routing": "br-gov-inep",
            "_source": data}
        actions_append(action)

        while True:
            try: # dump x number of objects at a time
                if len(actions) >= 100:
                    deque(parallel_bulk(esclient, actions), maxlen=0)
                    actions = []
                sleep(.01)
            except elasticsearch.helpers.BulkIndexError as e:
                pprint(str(e))
                sleep_seconds(60)
                # raise e
            else: break

    # check returned data
    # print(col.find({"$and":[queryStart,queryEnd]}).count())

    # set timestamp to next period
    timestamp_query = timestamp_query - (12*3600*1000)
    sleep_seconds(60)