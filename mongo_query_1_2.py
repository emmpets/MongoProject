import csv
import json
# import pandas as pd
# import sys, getopt, pprint
from pymongo import MongoClient
#CSV to JSON Conversion
# csvfile = open('/Users/Manos/projects/mongodb/microblogDataset_COMP6235_CW2.csv', 'r')
# reader = csv.DictReader( csvfile )
mongo_client=MongoClient('mongodb://localhost:27017/')
db=mongo_client.mydb

print(mongo_client)
print(db)
print("connected")


all_plithos=db.things.find().count()

print("all the messages count:", end=" ")
print(all_plithos)


print("question 1")
distinct_count = db.things.distinct('id_member')
print("The number of the distinct users:", end=" ")
print(len(distinct_count))


print("question 2")

pipeline=[
    {"$group": { "_id": "$id_member", "count": {"$sum": 1} }},
    {"$sort": { "count": -1 }},
    {"$limit": 10 }
    ]

result = list(db.things.aggregate(pipeline))

# print(result)

sum1 = 0
for plithos in result:
    sum1 = sum1 + plithos['count']
# print(sum1)
percentage =100*sum1/all_plithos
print("The  percentage  of the ALL messages of top ten user", end=" ")

print(percentage,end=" ")
print ("%")



