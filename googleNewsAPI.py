import requests
import json
import numpy as np
from newspaper import Article
from collections import defaultdict
from pyspark import SparkContext, SparkConf
from pyspark.ml.clustering import KMeans
import gmplot

conf = SparkConf().setMaster("local").setAppName("project")
sc = SparkContext(conf=conf)

# using Google News API for collecting articles
url = ('https://newsapi.org/v2/everything?'
       'q=Apple&'
       'from=2017-11-19&'
       'sortBy=popularity&'
       'apiKey=a562d2fdb25846999ae74f07e83d467d'
       )

response = requests.get(url)

data = response.json()

# print(data)

x1 = json.dumps(data)
jsontoPy = json.loads(x1)

dictUrl = {}
articleId = 1
for i in jsontoPy['articles']:
    articleUrl = i.get('url')
    dictUrl[articleId] = articleUrl
    articleId += 1

# print(dictUrl[1])
# print(articleUrl)

dictXY = {}
dictXY = defaultdict(list)
lat =[]
lng =[]
gmap = gmplot.GoogleMapPlotter(0, 0, 2,apikey=' AIzaSyDLddgAEB0qY8PLEHr-DF-YXPqoK3HdF7E ')

for i in dictUrl.keys():
    # print(dictUrl[i]+" ",i)
    article = Article(dictUrl[i])
    article.download()
    article.parse()
    articleText = article.text
    # print(articleText)

    # using Geoparser.io for location extraction from the articles
    url2 = 'https://geoparser.io/api/geoparser'
    headers = {'Authorization': 'apiKey 27103686864861756'}
    data2 = {'inputText': articleText}
    response2 = requests.post(url2, headers=headers, data=data2)
    jsonData2 = json.dumps(response2.json(), indent=2)
    # print(jsonData2)
    jsontoPy2 = json.loads(jsonData2)
    
    for j in jsontoPy2['features']:
        geoCoord = j.get('geometry').get('coordinates')
        # print(geoCoord)
        lat.append(geoCoord[0])
        lng.append(geoCoord[1])
        dictXY[i].append(geoCoord)

gmap.plot(lat, lng, 'cornflowerblue', edge_width=10)
gmap.draw('map.html')
# print(dictXY)

a = sc.parallelize(dictXY) # generate rdd of keys

def func(key):
  l = []  # empty list
  for i in range(len(dictXY.get(key))):
    l.append({key: dictXY.get(key)[i]})
  return l
  
b = a.flatMap(func)

b.take(20)

c = b.collect()

mat = []

for i in c:
    for j in i.values():
        mat.append(j)

print(mat)


# with open("/home/akshay/Documents/gNews.txt", 'w') as outfile:
#     json.dump(location, outfile)
