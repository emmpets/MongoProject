import pymongo
__author__ = 'Manos'
from pymongo import MongoClient
import sys


mongo_client=MongoClient('mongodb://localhost:27017/')
db=mongo_client.mydb
db_col=db.things


data=db.things.find()


def meanLength(data):

    sum_of_texts = 0
    for row in data:
        if 'text' in row:
            sum_of_texts = sum_of_texts + len(str(row["text"]))
    average_tweet_size = sum_of_texts/db.things.count()
    return average_tweet_size

print(meanLength(data))