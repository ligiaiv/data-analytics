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
from elasticsearch.helpers import bulk#, parallel_bulk
# from collections import deque
from pprint import pprint
from tqdm import tqdm
from time import time, sleep

from config import ES_URI, MG_URI

from lib_time import datetime_from_timestamp, datetime_from_str
from lib_time import datetime_to_timestamp, datetime_to_str
from lib_time import sleep_seconds
import math

# Define vars
esclient = Elasticsearch([ES_URI])
mgclient = MongoClient(MG_URI)

# max number of tweets sent at once to DB
MAX_TWEETS = 1000

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
time_ajust = 3600*24*3
timestamp_now = (timestamp_now * 1000) -time_ajust
timestamp_end = (timestamp_end * 1000) -time_ajust

# set to query tweets from now
timestamp_query = timestamp_now

# pull from mongo and dump into ES using bulk API
actions = []; actions_append = actions.append

# start_later = True
# tweet_count = 0

for i in range(1,15):
    title = 'twitter-'+datetime_to_str(datetime_from_timestamp((timestamp_query/1000)-1),'%Y-%m-%d')
    print(str(i)+'/14:', datetime_from_timestamp(timestamp_query/1000, '%d-%m-%Y'), '->', title)
    queryStart = {'status.timestamp_ms': {"$gte": timestamp_query-(12*3600*1000)}}
    queryEnd = {'status.timestamp_ms': {"$lte": timestamp_query}}
    query = {"$and":[queryStart,queryEnd]}
    col_find = col.find(query)
    col_total = col_find.count()
    pprint(query)
    for data in tqdm(col_find, total=col_total):

        # if start_later:
        #     tweet_count += 1
        #     if tweet_count < 211500:
        #         continue
        #     else: start_later = False

        # fixes from Mongo to Elasticsearch
        data.pop('_id')
        data = dict(data)


        data['status']['timestamp_ms'] = int(data['status']['timestamp_ms'])
        geocode = data['reverse_geocode']

        # try:
        #     # data['status']['entities']['user_mentions'][0]['name']=None
        #     # print(type(data['status']['entities']['user_mentions'][0]['id']))
        #     # data['status']['entities']['user_mentions'][0]['id'] = int(data['status']['entities']['user_mentions'][0]['id'])
        #     # print(type(data['status']['entities']['user_mentions'][0]['id']))
        # except Exception as e:
        #     data['status']['entities']['user_mentions']=None

        #     raise e

        if isinstance(geocode,list):
            temp = geocode[0]
            geocode[0]=geocode[1]
            geocode[1]=temp
            data['reverse_geocode'] = geocode
        else: data['reverse_geocode'] = None

        if(data['status']['place'] is not None ):
            if math.isnan(data['status']['place']['id']):
                data['status']['place']['id']=None

        if 'retweeted_status' in data['status'].keys() :

            if(data['status']['retweeted_status']['place'] is not None ):
                if math.isnan(data['status']['retweeted_status']['place']['id']):
                    data['status']['retweeted_status']['place']['id']=None


            if 'quoted_status' in data['status']['retweeted_status'].keys() :
                if(data['status']['retweeted_status']['quoted_status']['place'] is not None ):
                    if math.isnan(data['status']['retweeted_status']['quoted_status']['place']['id']):
                        data['status']['retweeted_status']['quoted_status']['place']['id']=None

        if 'quoted_status' in data['status'].keys() :
            if(data['status']['quoted_status']['place'] is not None ):
                if math.isnan(data['status']['quoted_status']['place']['id']):
                    data['status']['quoted_status']['place']['id']=None



        action = {
            "_index": title,
            "_type": "statuses",
            "_routing": "br-gov-inep",
            "_source": data}
        actions_append(action)
        print
        while True:
            try: # dump x number of objects at a time
                # print('is_str  ',data['status']['id_str'])
                # print('place  ',data['status']['place'])
                # print(len(actions))
                if len(actions) >= MAX_TWEETS:
                    # deque(parallel_bulk(esclient, actions), maxlen=0)
                    bulk(esclient, actions)
                    print('Sent ',MAX_TWEETS,' tweets to DataBase')
                    actions = []; actions_append = actions.append
                    # quit()
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