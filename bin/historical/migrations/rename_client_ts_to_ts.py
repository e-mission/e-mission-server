#Modifies the Stage_client_stats collection of Stage_database.
#Changes the client_ts field to ts.


from pymongo import MongoClient
client = MongoClient()
db = client.Stage_database
collection = db.Stage_client_stats
collection.update({}, { '$rename': {"client_ts": "ts"}}, multi=True)
