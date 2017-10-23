import random
import urllib

def list2String(x):
	""" from a list like [1,2,5]
    return a string like '1,2,5' """
	data = ""
	for i in x:
		data += str(i)+","
	return data[0:len(data)-1]

def makeChart(x,y,filename):
	query_url = "http://chart.apis.google.com/chart?chxt=x,y&chs=600x400&cht=s&chd=t:"
	query_url += list2String(x)+"|"+list2String(y)
	chart = urllib.urlopen( query_url) # retrieve the chart
	print("saving",query_url)
	f = open(filename,"wb")
	f.write(chart.read()) # save the pic
	f.close()

x = random.sample(range(0,100),10) # list with
y = random.sample(range(0,100),10) # random values in [0 100[
makeChart(x,y,"chart.png")