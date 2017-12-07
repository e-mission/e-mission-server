from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
#Modifies the Stage_client_stats collection of Stage_database.
#Changes the client_ts field to ts.


from future import standard_library
standard_library.install_aliases()
from builtins import *
from pymongo import MongoClient
client = MongoClient()
db = client.Stage_database

collection = db.Stage_client_stats
collection.update({}, { '$rename': {"client_ts": "ts"}}, multi=True)

collection = db.Stage_server_stats
collection.update({}, { '$rename': {"client_ts": "ts"}}, multi=True)

collection = db.Stage_result_stats
collection.update({}, { '$rename': {"client_ts": "ts"}}, multi=True)

