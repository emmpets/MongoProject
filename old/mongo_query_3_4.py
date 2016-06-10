import pymongo

__author__ = 'Manos'

import csv
from optparse import OptionParser



from pymongo import MongoClient
from datetime import datetime
import time

mongo_client=MongoClient('mongodb://localhost:27017/')
db=mongo_client.mydb
db_col=db.things



result3a = db.things.find({},{"timestamp": 1} ).sort("timestamp",pymongo.DESCENDING).limit(1)

print("query 3")
for row in result3a:
    print("The last message published on:", end=" ")
    print(row["timestamp"])

result3b = db.things.find({"timestamp":{'$ne':None}},{"timestamp": 1} ).sort("timestamp",pymongo.ASCENDING).limit(1)

for rb in result3b:
    print("The earliest message published on:", end=" ")
    print(rb["timestamp"])



print("query 4")

tmax = row["timestamp"]
dmax = datetime.strptime(tmax, "%Y-%m-%d %H:%M:%S")
secondmax=time.mktime(dmax.timetuple())


tmin = rb["timestamp"]
dmin = datetime.strptime(tmin, "%Y-%m-%d %H:%M:%S")
secondmin=time.mktime(dmin.timetuple())


# print(secondmax)
all_plithos_msg=db.things.find().count()
deltatimemean = ((secondmax - secondmin) / (all_plithos_msg -1))
print("The mean time delta between all messages is :", end=" ")
print(deltatimemean)


