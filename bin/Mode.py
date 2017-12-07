from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
__author__ = 'Yin'
from pymongo import MongoClient
from get_database import get_mode_db, get_section_db, get_trip_db

Modes=get_mode_db()
for items in Modes.find():
    Modes.remove()
modes_todo={ 'mode_id': 1,'mode_name':'walking'}
Modes.insert(modes_todo)
modes_todo={ 'mode_id': 2,'mode_name':'running'}
Modes.insert(modes_todo)
modes_todo={ 'mode_id': 3,'mode_name':'cycling'}
Modes.insert(modes_todo)
modes_todo={ 'mode_id': 4,'mode_name':'transport'}
Modes.insert(modes_todo)
modes_todo={ 'mode_id': 5,'mode_name':'bus'}
Modes.insert(modes_todo)
modes_todo={ 'mode_id': 6,'mode_name':'train'}
Modes.insert(modes_todo)
modes_todo={ 'mode_id': 7,'mode_name':'car'}
Modes.insert(modes_todo)
modes_todo={ 'mode_id': 8,'mode_name':'mixed'}
Modes.insert(modes_todo)
modes_todo={ 'mode_id': 9,'mode_name':'air'}
Modes.insert(modes_todo)
