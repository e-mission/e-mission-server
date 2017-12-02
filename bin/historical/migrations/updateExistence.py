from moves import Moves
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from datetime import datetime, timedelta
import logging
import pytz
import json
from dateutil import parser
from get_database import get_mode_db, get_section_db, get_trip_db, get_moves_db
from time import sleep
from collect import label_filtered_section

config_data = json.load(open('config.json'))
log_base_dir = config_data['paths']['log_base_dir']
logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s',
                    filename="%s/moves_collect.log" % log_base_dir, level=logging.DEBUG)

def updateExistence():
  Sections = get_section_db()
  for tempSec in Sections:
    if 'retained' not in tempSec.keys() or tempSec['retained'] is None:
		label_filtered_section(tempSec)
    else:
        #don't update
        pass
