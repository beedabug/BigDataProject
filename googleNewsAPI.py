import json
from collections import defaultdict

import gmplot
import requests
from newspaper import Article
from pyspark import SparkContext, SparkConf
from pyspark.ml.clustering import KMeans
from pyspark.ml.linalg import Vectors
from pyspark.sql import SparkSession

# setting up Spark Context
conf = SparkConf().setMaster("local").setAppName("project")
sc = SparkContext(conf=conf)
spark = SparkSession \
    .builder \
    .getOrCreate()

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

# json to python dict 
x1 = json.dumps(data)
jsontoPy = json.loads(x1)

# extract the url from the articles
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

# using Google Map API for mapping the coordinates
gmap = gmplot.GoogleMapPlotter(0, 0, 2,apikey=' AIzaSyDLddgAEB0qY8PLEHr-DF-YXPqoK3HdF7E ')

text = {}
for i in dictUrl.keys():
    # print(dictUrl[i]+" ",i)
    article = Article(dictUrl[i])
    article.download()
    article.parse()
    articleText = article.text
    text[i] = article.text  # save article text
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
        lat.append(geoCoord[1])
        lng.append(geoCoord[0])
        dictXY[i].append(geoCoord)
       
gmap.plot(lat, lng, 'cornflowerblue', edge_width=10)
gmap.draw('map.html')
# print(dictXY)

articles = sc.parallelize(text)  # create rdd
words = articles.map(lambda x: {x: text[x].split(" ")})  
# words = [{1: ['Postmates,', 'the', 'get-anything-delivered', 'service,', ..., 'word_n']}, {2: ['word1', 'word2', ..., 'word_n']}]   

def func(key, d):
  l = []  # empty list
  for i in range(len(d.get(key))):
    l.append({key: d.get(key)[i]})
  return l

a = sc.parallelize(dictXY) # generate rdd of keys  
b = a.flatMap(lambda x: func(x, dictXY))
c = b.collect()

mat = []  # create matrix for latitude/longitude coordinates
for i in c:
    for j in i.values():
        mat.append(j)

print(mat)

data = [(Vectors.dense(x),) for x in mat]
d = spark.createDataFrame(data, ["features"])
kmeans = KMeans(k=10, seed=1)
model = kmeans.fit(d)
transformed = model.transform(d).select("features", "prediction")
print(transformed.rdd.take(10))
rows = transformed.rdd
predictedRDD = rows.map(lambda x: (x.prediction, [x.features[0], x.features[1]]))
print(predictedRDD.take(10))
centers = model.clusterCenters()
print("Cluster Centers: ")
for center in centers:
    print(center)
centers = model.clusterCenters()
print("Cluster Centers: ")
for center in centers:
    print(center)

begin = 'https://maps.googleapis.com/maps/api/geocode/json?latlng='
end = '&key=AIzaSyCjn7gFXea2AhmAae51wIwseBZY4CKmscA'

responses = []
for c in centers:
    url = begin + str(c[0]) + ',' + str(c[1]) + end
    responses.append(requests.post(url).json())

cluster_locations = []
for i in range(len(responses)):
    jsonString = json.dumps(responses[i])  # convert json object to string
    jsontoPy = json.loads(jsonString)  # convert string to dictionary object
    cluster_locations.append({i+1: jsontoPy['results'][0]['formatted_address']})

# print(cluster_locations)

# with open("/home/akshay/Documents/gNews.txt", 'w') as outfile:
#     json.dump(location, outfile)
