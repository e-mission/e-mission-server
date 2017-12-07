from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
__author__ = 'Yin'
from pymongo import MongoClient
from get_database import get_user_db

OldUsers=MongoClient('localhost').Test_database.moves_user_access
Users=get_user_db()

for olduser in OldUsers.find():
    Users.insert(olduser)