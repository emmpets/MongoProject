__author__ = 'Manos'

import re
from pymongo import MongoClient
import sys
# len(re.findall("#", str(row["text"])))


mongo_client=MongoClient('mongodb://localhost:27017/')
db=mongo_client.mydb
db_col=db.things


data=db.things.find()

def meanLength(data):
    sum_of_hashes_per_text = 0
    for row in data:
        if 'text' in row:
            sum_of_hashes_per_text+=str(row['text']).count('#')
    print("The average number of hashatag which is contained in every message is :", end=" ")
    average_hashes_size = sum_of_hashes_per_text/db.things.count()
    return average_hashes_size

print(meanLength(data))
