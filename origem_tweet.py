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
queryAndroid = {"status.source": {
					"$regex":"Android"
					}
				}
queryIOS = {"$or":
			[
				{"status.source":{"$regex":"iPhone"}},
				{"status.source":{"$regex":"iPad"}},
				{"status.source":{"$regex":"iOS"}}
			]}
queryWindowsPhone = {"status.source": {"$regex":"Windows Phone"}}

queryWebClient = {"$or":
				[
					{"status.source":{"$regex":"Web Client"}},
					{"$and":
						[
							{"status.source":{"$regex":"Windows"}},
							{"$nor":#nor(A,A) = not(A), por algum motivo não aceita not, então usei 2 nor's
								[
									{"status.source":{"$regex":"Windows Phone"}},
									{"status.source":{"$regex":"Windows Phone"}}
								]
							}
						]
					}
				]}
queryMobileWeb = {"status.source":{"$regex":"Mobile Web"}}

queryOutros = {"$nor":
				[
					{"status.source":{"$regex":"Web Client"}},
					{"status.source":{"$regex":"Windows"}},
					{"status.source":{"$regex":"Android"}},
					{"status.source":{"$regex":"iPhone"}},
					{"status.source":{"$regex":"iPad"}},
					{"status.source":{"$regex":"iOS"}},
					{"status.source":{"$regex":"Windows Phone"}},
					{"status.source":{"$regex":"Mobile Web"}}
				]}

# data = [
# 	{"regiao":"Norte","estados":[
# 		{"estado":"ac"},
# 		{"estado":"am"},
# 		{"estado":"ap"},
# 		{"estado":"pa"},
# 		{"estado":"ro"},
# 		{"estado":"rr"},
# 		{"estado":"to"}
# 		]},
# 	{"regiao":"Nordeste","estados":[
# 		{"estado":"al"},
# 		{"estado":"ba"},
# 		{"estado":"ce"},
# 		{"estado":"ma"},
# 		{"estado":"pb"},
# 		{"estado":"pe"},
# 		{"estado":"pi"},
# 		{"estado":"rn"},
# 		{"estado":"se"}
# 		]},
# 	{"regiao":"Centro-Oeste","estados":[
# 		{"estado":"go"},
# 		{"estado":"ms"},
# 		{"estado":"mt"}
# 		]},
# 	{"regiao":"Sudeste","estados":[
# 		{"estado":"es"},
# 		{"estado":"mg"},
# 		{"estado":"rj"},
# 		{"estado":"sp"}
# 		]},
# 	{"regiao":"Sul","estados":[
# 		{"estado":"pr"},
# 		{"estado":"rs"},
# 		{"estado":"sc"}
# 		]}
# ]

nAndroid = db.tweets.find({"$and":[queryTempo,queryAndroid]}).count()
nIOS = db.tweets.find({"$and":[queryTempo,queryIOS]}).count()
nWinPhone = db.tweets.find({"$and":[queryTempo,queryWindowsPhone]}).count()
nWebClient = db.tweets.find({"$and":[queryTempo,queryWebClient]}).count()
nMobWeb = db.tweets.find({"$and":[queryTempo,queryMobileWeb]}).count()
nOutros = db.tweets.find({"$and":[queryTempo,queryOutros]}).count()

out_data={
	"Android":nAndroid,
	"IOS":nIOS,
	"Windows Phone":nWinPhone,
	"Web":nWebClient,
	"Mobile Web":nMobWeb,
	"Outros":nOutros
}

print ("nAndroid  \t"+str(nAndroid))
print ("nIOS      \t"+str(nIOS))
print ("nWinPhone \t"+str(nWinPhone))
print ("nWebClient\t"+str(nWebClient))
print ("nMobileWeb\t"+str(nMobWeb))
print ("nOutros   \t"+str(nOutros))


print( "\nA soma e :        "+str(nAndroid+nIOS+nWinPhone+nWebClient+nMobWeb+nOutros))
# estado["tecnologias"] = {"Android":nAndroid,"iOS":nIOS,"Windows Phone":nWinPhone,"WebClient":nWebClient,"Mobile Web":nMobWeb,"Outros":nOutros}


with open("origem_tweet.json", 'w') as outfile:
    json.dump(out_data, outfile)
