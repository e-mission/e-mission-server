__author__ = 'Yin'
from pymongo import MongoClient
from get_database import get_user_db

OldUsers=MongoClient('localhost').Test_database.moves_user_access
Users=get_user_db()

for olduser in OldUsers.find():
    Users.insert(olduser)