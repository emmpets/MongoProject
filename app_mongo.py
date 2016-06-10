from pymongo import MongoClient
import pymongo
from datetime import datetime,time
import time
from bson.code import Code

mongo_client=MongoClient('mongodb://localhost:27017/')
db=mongo_client.mydb
db_col=db.things
dbc = mongo_client.mydb.things

print mongo_client
print(db)
print("connected")


def first_querry():

    all_count = db.things.find().count()
    return all_count

def second_querry():

    allusers = first_querry()
    pipeline = [
        {"$group": {"_id": "$id_member", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 10}
    ]

    result = list(db.things.aggregate(pipeline))

    sum1 = 0
    for plithos in result:
        sum1 = sum1 + plithos['count']

    percentage = 100.0 * sum1 / allusers
    return percentage

def third_querry():

    result3a = db.things.find({}, {"timestamp": 1}).sort("timestamp", pymongo.DESCENDING).limit(1)
    for row in result3a:
        # print("The last message published on:"),
        str(row["timestamp"])
    tmax = row["timestamp"]

    result3b = db.things.find({"timestamp": {'$ne': None}}, {"timestamp": 1}).sort("timestamp",
                                                                                   pymongo.ASCENDING).limit(1)

    for rb in result3b:
        # print("The earliest message published on:"),
        str(rb["timestamp"])

    tmin = rb["timestamp"]
    return (tmax,tmin)

def fourth_querry():

    tmax, tmin = third_querry()
    dmax = datetime.strptime(tmax, "%Y-%m-%d %H:%M:%S")
    secondmax = time.mktime(dmax.timetuple())

    dmin = datetime.strptime(tmin, "%Y-%m-%d %H:%M:%S")
    secondmin = time.mktime(dmin.timetuple())


    all_plithos_msg = db.things.find().count()
    deltatimemean = ((secondmax - secondmin) / (all_plithos_msg - 1))
    return deltatimemean


data = dbc.find()
def fifth_querry(data):

    sum_of_texts = 0
    for row in data:
        if 'text' in row:

            sum_of_texts += len(str(row["text"]).encode('utf-8'))
    average_tweet_size = sum_of_texts / db.things.count()
    return average_tweet_size

def sixth_querry():

    mapperUni = Code("""
    function() {
    var thisText = this.text;
    var splitStr = thisText.toString().split(" ");
    for(i=0 ; i< splitStr.length ;i++){
        var clean1 = splitStr[i].replace(/[.,-\/#!$%\^&\*;:{}=\-_`~()]/g,"");
        var clean2 = clean1.replace(/\s{2,}/g," ");
        var cleanStr = clean2.trim();
            if (cleanStr.length>0)
            emit(cleanStr,1);
            }
    }
    """)

    reducerUni = Code("""
    function(key, value) {
    return Array.sum(value);
    }
    """)

    unigram_counter = dbc.map_reduce(mapperUni, reducerUni, 'uniCounter')
    unigram_list = list(db.uniCounter.find().sort('value', -1).limit(10))
    for uni in unigram_list:
        print ('Unigram' + uni['_id'] + 'has' + str(uni['value']) + 'appearances')


def seventh_querry():

    mapperBi = Code("""
    		function() {
    			var tempText = this.text;
    			var splitText = tempText.toString().split(" ");
    			for(i=0 ; i<splitText.length-1 ;i++){
    				punctText = splitText[i].trim();
    				punctText2 = splitText[i+1].trim();
    				var punctRem = punctText.replace(/[.,-\/#!$%\^&\*;:{}=\-_`~()]/g,"");
    				var punctRem2 = punctText2.replace(/[.,-\/#!$%\^&\*;:{}=\-_`~()]/g,"");
    				var firstStr = punctRem.replace(/\s{2,}/g," ");
    				var secStr = punctRem2.replace(/\s{2,}/g," ");

    				finalStr = (firstStr + ' ' + secStr).trim();
    				if (finalStr !== '')
    					emit(finalStr,1);
    			}
    		}
    """)

    reducerBi = Code("""
    		function(key, value) {
    			return Array.sum(value);
    		}
    """)

    bigram_counter = dbc.map_reduce(mapperBi, reducerBi, 'bigramCounter')
    bigram_list = list(db.bigramCounter.find().sort('value', -1).limit(10))

    for bigrams in bigram_list:
        print ('Bigram' + bigrams['_id'] + 'has' + str(bigrams['value']) + 'appearances')

def eight_querry(data):

    sum_of_hashes_per_text = 0
    for row in data:
        if 'text' in row:
            sum_of_hashes_per_text += str(row['text']).count('#')

    average_hashes_size = sum_of_hashes_per_text / db.things.count()
    return average_hashes_size

def ninth_querry():

    mapperMap = Code("""
    		function() {
    			var ukCenterLat = '54.749991';
    			var ukCenterLng = '-3.867188';
    			var currentLng = this.geo_lng;
    			var currentLat = this.geo_lat;
    			var loc = "";
    			if (currentLng < ukCenterLng && currentLat >= ukCenterLat) {
    					loc = "North-West";
    			}else if(currentLng < ukCenterLng && currentLat < ukCenterLat){
    				loc = "South-West";
    			}else if (currentLng >= ukCenterLng && currentLat >= ukCenterLat) {
    					loc = "North-East";
    			}else if (currentLng >= ukCenterLng && currentLat < ukCenterLat){
    					loc = "South-East";
    			}
    			emit(loc, 1);
    		}
    """)

    reducerMap = Code("""
    		function(key, value) {
    			return Array.sum(value);
    		}
    """)

    LocationCounter = dbc.map_reduce(mapperMap, reducerMap, 'geoLocDistr')
    topLocation = db.geoLocDistr.find().sort('value', -1).limit(1)

    print('Most of the messages were published in' + topLocation[0]['_id'] + ' with ' + str(
        topLocation[0]['value']) + ' tweets')



ans=True
while ans:
    print("""
        1.How many unique users are there?
        2.How many tweets (%) did the top 10 users (measured by the number of messages) publish?
        3.What was the earliest and latest date (YYYY-MM-DD HH:MM:SS) that a message was published?
        4.What is the mean time delta between all messages?
        5.What is the mean length of a message?
        6.What are the 10 most common unigram within the messages?
        7.What are the 10 most common bigram within the messages?
        8.What is the average number of hashtags (#) used within a message?
        10.Exit/Quit


        """)
    ans = raw_input("What would you like to do? ")
    if ans == "1":
        print "The summary of all unique users is: ", first_querry()
    elif ans == "2":
        print("The percentage of the ALL messages of top ten user"), second_querry(), "%",
    elif ans == "3":
        print"The last message published on:", third_querry()[0]
        print"The earliest message published on:", third_querry()[1]
    elif ans == "4":
        print"The mean time delta between all messages is :", fourth_querry()
    elif ans == "5":
        print"The mean length of the messages is :", fifth_querry(data)
    elif ans == "6":
        print"The 10 most common unigrams within the messages are:", sixth_querry()
    elif ans == "7":
        print"The 10 most common bigrams within the messages are:", seventh_querry()
    elif ans == "8":
        print"The average number of hashtags (#) used within a message is:", eight_querry(data)
    elif ans == "9":
        ninth_querry()
    elif ans == "10":
        print("\n Goodbye")
        ans = None
    else:
        print("\n Not Valid Choice Try again")


