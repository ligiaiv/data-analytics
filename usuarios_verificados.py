import pymongo,json,time,datetime,re
from bson import json_util,datetime
from pymongo import MongoClient
from collections import Counter
import sys

client = MongoClient()
client = MongoClient("mongodb://188.166.40.27:27017/")

time_window_str = sys.argv[1]
if time_window_str == "1h":
	hours_back = 1
elif time_window_str == "12h":
	hours_back = 12
elif time_window_str == "1d":
	hours_back = 24
elif time_window_str == "7d":
	hours_back = 24*7

else:
	print('You typed ',time_window_str)
	print('Only possible options: 1h, 12h, 1d, 7d')
	quit()
db = client['twixplorer']
tweets = db['tweets']


now = int(time.time())*1000
init_analyse_time = now - hours_back*60*60*1000
# before = 0


queryTempo = {'status.timestamp_ms': {
	               "$gte": init_analyse_time
	            }}
query_verified = {"status.user.verified":true}
query_not_verified= {"status.user.verified":false}


verified = db.tweets.find({"$and":[queryTempo,query_verified]}).count()

not_verified = db.tweets.find({"$and":[queryTempo,query_not_verified]}).count()


with open("usuarios_verificados.json", 'w') as outfile:
    json.dump(out_data, outfile)
