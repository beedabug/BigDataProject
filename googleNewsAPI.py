
import requests
import json

url = ('https://newsapi.org/v2/everything?'
       'q=Apple&'
       'from=2017-11-19&'
       'sortBy=popularity&'
       'apiKey=a562d2fdb25846999ae74f07e83d467d'
       )

response = requests.get(url)

#print(response.json())

data = response.json()

with open("/home/akshay/Documents/gNews1.txt", 'w') as outfile:
    json.dump(data, outfile)
