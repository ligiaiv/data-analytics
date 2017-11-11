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
esclient = Elasticsearch()#Elasticsearch([ES_URI])
mgclient = MongoClient(MG_URI)

# max number of tweets sent at once to DB
MAX_TWEETS = 1000

DATABASE = 'inep'
EXCEPTION_FILE = 'tweets_not_sent'
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
# time_ajust = 3600*24*3
time_ajust = 0
timestamp_now = (timestamp_now * 1000) -time_ajust
timestamp_end = (timestamp_end * 1000) -time_ajust

# set to query tweets from now
timestamp_query = timestamp_now

# pull from mongo and dump into ES using bulk API
actions = []; actions_append = actions.append

# start_later = True
# tweet_count = 0


# Creates file to note which tweets were not sent

file_exceptions=open(EXCEPTION_FILE, 'w')
file_exceptions.close()



def fix_country_code(country_code):


    if(type(country_code) is dict):
        country_code_str = ''
        for key in sorted(country_code.keys()):
            country_code_str += country_code[key]
    elif(type(country_code) is str):
        country_code_str=country_code


    return country_code_str

def fix_place(data,reason):
    reason=reason.replace('failed to parse [','').replace(']','')
    try:
        data['status']['place']['bounding_box']['coordinates'][0][1][1]+=1#+data['status']['place']['bounding_box']['coordinates'][0][1][1]
        data['status']['place']['bounding_box']['coordinates'][0][2][1]+=1#+data['status']['place']['bounding_box']['coordinates'][0][2][1]
        data['status']['place']['bounding_box']['coordinates'][0][2][0]+=1#+data['status']['place']['bounding_box']['coordinates'][0][2][0]
        data['status']['place']['bounding_box']['coordinates'][0][3][0]+=1#+data['status']['place']['bounding_box']['coordinates'][0][3][0]
    except Exception as e:
        pass

    try:

        data['status']['retweeted_status']['place']['bounding_box']['coordinates'][0][1][1]+=1#+data['status']['retweeted_status']['place']['bounding_box']['coordinates'][0][1][1]
        data['status']['retweeted_status']['place']['bounding_box']['coordinates'][0][2][1]+=1#+data['status']['retweeted_status']['place']['bounding_box']['coordinates'][0][2][1]
        data['status']['retweeted_status']['place']['bounding_box']['coordinates'][0][2][0]+=1#+data['status']['retweeted_status']['place']['bounding_box']['coordinates'][0][2][0]
        data['status']['retweeted_status']['place']['bounding_box']['coordinates'][0][3][0]+=1#+data['status']['retweeted_status']['place']['bounding_box']['coordinates'][0][3][0]

        # bounding_box = data['status']['retweeted_status']['place']['bounding_box']
    except Exception as e:
        pass

    try:
        data['status']['quoted_status']['place']['bounding_box']['coordinates'][0][1][1]+=1#data['status']['quoted_status']['place']['bounding_box']['coordinates'][0][1][1]
        data['status']['quoted_status']['place']['bounding_box']['coordinates'][0][2][1]+=1#data['status']['quoted_status']['place']['bounding_box']['coordinates'][0][2][1]
        data['status']['quoted_status']['place']['bounding_box']['coordinates'][0][2][0]+=1#data['status']['quoted_status']['place']['bounding_box']['coordinates'][0][2][0]
        data['status']['quoted_status']['place']['bounding_box']['coordinates'][0][3][0]+=1#data['status']['quoted_status']['place']['bounding_box']['coordinates'][0][3][0]
        # bounding_box = data['status']['quoted_status']['place']['bounding_box']
    except Exception as e:
        pass
    try:

        data['status']['retweeted_status']['quoted_status']['place']['bounding_box']['coordinates'][0][1][1]+=1#data['status']['retweeted_status']['quoted_status']['place']['bounding_box']['coordinates'][0][1][1]
        data['status']['retweeted_status']['quoted_status']['place']['bounding_box']['coordinates'][0][2][1]+=1#data['status']['retweeted_status']['quoted_status']['place']['bounding_box']['coordinates'][0][2][1]
        data['status']['retweeted_status']['quoted_status']['place']['bounding_box']['coordinates'][0][2][0]+=1#data['status']['retweeted_status']['quoted_status']['place']['bounding_box']['coordinates'][0][2][0]
        data['status']['retweeted_status']['quoted_status']['place']['bounding_box']['coordinates'][0][3][0]+=1#data['status']['retweeted_status']['quoted_status']['place']['bounding_box']['coordinates'][0][3][0]

        # bounding_box = data['status']['place']['bounding_box']
    except Exception as e:
        pass

    return data


def send_to_DB(actions, counter_times_tried):
    counter_times_tried+=1

    try: # dump x number of objects at a time

        # deque(parallel_bulk(esclient, actions), maxlen=0)
        bulk(esclient, actions)
        print('Sent ',MAX_TWEETS,' tweets to DataBase')
        # actions = []; actions_append = actions.append
        sleep(.01)

    except elasticsearch.helpers.BulkIndexError as e:

        pprint(e.errors)

        if counter_times_tried < 2:
            for erro in e.errors:
                if('Provided shape has duplicate consecutive coordinates' in erro['index']['error']['caused_by']['reason']):
                    for action in actions:
                        if action['_id']==erro['index']['_id']:
                            action['_source']=fix_place(action['_source'],erro['index']['error']['reason'])
            send_to_DB(actions,counter_times_tried)
        # If that doesn't work we try to remove the bad action, note its _id and try to send the rest
        elif counter_times_tried ==2:
            exception_file = open(EXCEPTION_FILE,'a')
            for erro in e.errors:
                exception_file.write(erro['index']['_id']+'\n')
                for action in actions:
                        if action['_id']==erro['index']['_id']:
                            actions.remove(action)
            send_to_DB(actions,counter_times_tried)
        #  And if that doesn't wore we will just write down all the ids not sent and give up on sending this package
        else:
            with open(EXCEPTION_FILE,'a') as exception_file:
                for action in actions:
                    exception_file.write(action['_id']+'\n')





def Prepare_Data(data):

    # FIX TIMESTAMP
    data['status']['timestamp_ms'] = int(data['status']['timestamp_ms'])
    geocode = data['reverse_geocode']

    # FIX GEOCODE
    if isinstance(geocode,list):
        temp = geocode[0]
        geocode[0]=geocode[1]
        geocode[1]=temp
        data['reverse_geocode'] = geocode
    else: data['reverse_geocode'] = None

    if(data['status']['place'] is not None ):
        if math.isnan(data['status']['place']['id']):
            data['status']['place']['id']=None
        data['status']['place']['country_code']=fix_country_code(data['status']['place']['country_code'])


    # FIX PLACE ID AND COUNTRY_CODE
    if 'retweeted_status' in data['status'].keys() :

        if(data['status']['retweeted_status']['place'] is not None ):
            if math.isnan(data['status']['retweeted_status']['place']['id']):
                data['status']['retweeted_status']['place']['id']=None
            data['status']['retweeted_status']['place']['country_code']=fix_country_code(data['status']['retweeted_status']['place']['country_code'])

        if 'quoted_status' in data['status']['retweeted_status'].keys() :
            if(data['status']['retweeted_status']['quoted_status']['place'] is not None ):
                if math.isnan(data['status']['retweeted_status']['quoted_status']['place']['id']):
                    data['status']['retweeted_status']['quoted_status']['place']['id']=None
                data['status']['retweeted_status']['quoted_status']['place']['country_code']=fix_country_code(data['status']['retweeted_status']['quoted_status']['place']['country_code'])

    if 'quoted_status' in data['status'].keys() :
        if(data['status']['quoted_status']['place'] is not None ):
            if math.isnan(data['status']['quoted_status']['place']['id']):
                data['status']['quoted_status']['place']['id']=None
            data['status']['quoted_status']['place']['country_code']=fix_country_code(data['status']['quoted_status']['place']['country_code'])

    return data


# MAIN()
for i in range(1,15):
    title = 'twitter-'+datetime_to_str(datetime_from_timestamp((timestamp_query/1000)-1),'%Y-%m-%d')
    print(str(i)+'/14:', datetime_from_timestamp(timestamp_query/1000, '%d-%m-%Y'), '->', title)
    queryStart = {'status.timestamp_ms': {"$gte": timestamp_query-(12*3600*1000)}}
    queryEnd = {'status.timestamp_ms': {"$lte": timestamp_query}}
    query = {"$and":[queryStart,queryEnd]}
    col_find = col.find(query)
    col_total = col_find.count()



    for data in tqdm(col_find, total=col_total):

        _id = str(data['_id'])
        data.pop('_id')
        data = dict(data)

        # fixes from Mongo to Elasticsearch

        data = Prepare_Data(data)
        action = {
        "_index": title,
        "_type": "statuses",
        "_routing": "br-gov-inep",
        "_source": data,
        "_id": _id}
        actions_append(action)

        # print('-------------------TWEETS THIS ROUND : ',len(tweets_this_round))

        if len(actions) >= MAX_TWEETS:

            send_to_DB(actions,0)

            actions = []; actions_append = actions.append


    # set timestamp to next period
    timestamp_query = timestamp_query - (12*3600*1000)
    sleep_seconds(60)
