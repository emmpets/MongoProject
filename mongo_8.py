__author__ = 'Manos'

from pymongo import MongoClient

mongo_client=MongoClient('mongodb://localhost:27017/')
db=mongo_client.mydb

print(mongo_client)
print(db)
print("connected")


#
# locs=[{
#   "locs" : [{"lng": "$geo_lng" , "lat":"$geo_lat" } ]
# }]
#
# locs=db.things.save(locs)




#
# geolocation=[
#                     {"$project": { "geolocation": {"$concat": ["$geo_lat", " , ", "$geo_lng" ] } } }
#             ]


# result = db.things.aggregate(geolocation)
#
# print(result)







#
# querry8= [
#                      {"loc": {["$geo_lat","$geo_lng"]}},
#                          {
#                            "$geoWithin":{
#                                "$geometry": {"type" : "Polygon" ,
#                                  "coordinates":[ [ [0, 0], [ 3, 6 ], [ 6, 1 ], [ 0, 0 ] ] ]
#                               }
#                            }
#                          }
#
#                         ]
# print= db.things.find(querry8)

#
# print("question 1")
# distinct_count = db.things.distinct('geo_lng')
# print("The number of the distinct lat:" )
# print(len(distinct_count))

# result = db.things.aggregate(
#             [
#                 {"$group": { "_id": {"lang": "$geo_lng", "lat": "$geo_lat" } } }
#             ]
#         )
# print(db.things.find(result))




