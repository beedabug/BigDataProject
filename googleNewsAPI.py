
import requests
import json
from newspaper import Article
from collections import defaultdict

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
        dictXY[i].append(geoCoord)

print(dictXY)

# with open("/home/akshay/Documents/gNews.txt", 'w') as outfile:
#     json.dump(location, outfile)
