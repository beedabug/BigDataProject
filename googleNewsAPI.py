
import requests
import json
from newspaper import Article
# from pyspark import SparkConf, SparkContext

url = ('https://newsapi.org/v2/everything?'
       'q=Apple&'
       'from=2017-11-19&'
       'sortBy=popularity&'
       'apiKey=a562d2fdb25846999ae74f07e83d467d'
       )

response = requests.get(url)

data = response.json()

# print(data)

jsonString = json.dumps(data)
jsontoPy = json.loads(jsonString)

# conf = SparkConf().setAppName("projectNews").setMaster("local[2]")
# sc = SparkContext(conf)

# mapper = jsontoPy.map()
# print(jsontoPy['articles'][100]['url'])

dictUrl = {}
articleId = 1
for i in jsontoPy['articles']:
    articleUrl = i.get('url')
    dictUrl[articleId] = articleUrl
    articleId += 1

# print(dictUrl)


article = Article(articleUrl)

article.download()
article.parse()
articleText = article.text
# print(articleText)

# using geoparser for location extraction from the articles
url2 = 'https://geoparser.io/api/geoparser'
headers = {'Authorization': 'apiKey 27103686864861756'}
data2 = {'inputText': articleText}
response2 = requests.post(url2, headers=headers, data=data2)
print(json.dumps(response2.json(), indent=4))




# with open("/home/akshay/Documents/gNews.txt", 'w') as outfile:
#   json.dump(data, outfile)

