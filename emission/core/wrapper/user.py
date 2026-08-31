from builtins import *
from builtins import object
import json
import logging
from datetime import datetime
import random
import requests

# Our imports
from emission.core.get_database import get_profile_db, get_uuid_db

defaultCarFootprint = 278.0 / 1609
defaultMpg = 8.91 / (1.6093 * defaultCarFootprint) # Should be roughly 32

# TODO: potentially pull this out into a separate module
# And have the words be i18n and downloaded from github on startup
# and have the word list be part of the dynamic config
ENGLISH_WORD_LIST = "https://raw.githubusercontent.com/dwyl/english-words/refs/heads/master/words_alpha.txt"

_ENGLISH_WORDS_CACHE = None

# Uses the singleton pattern to cache english words grouped by first character.
# Every cached word is alpha-only and at least 3 characters long.
def _load_words():
    global _ENGLISH_WORDS_CACHE
    if _ENGLISH_WORDS_CACHE is not None:
        return _ENGLISH_WORDS_CACHE

    words_by_first_char = {}
    response = requests.get(ENGLISH_WORD_LIST)
    if response.status_code != 200:
        response.raise_for_status()
        raise ValueError(response.status_code, "Failed to download ENGLISH_WORD_LIST from %s" % ENGLISH_WORD_LIST)
    for line in response.text.splitlines():
        word = line.strip().lower()
        if len(word) < 3 or not word.isalpha():
            continue
        first_char = word[0]
        words_by_first_char.setdefault(first_char, []).append(word)

    if len(words_by_first_char) < 2:
        raise ValueError(400, "Need valid words from at least two starting characters in ENGLISH_WORD_LIST")

    _ENGLISH_WORDS_CACHE = words_by_first_char
    return _ENGLISH_WORDS_CACHE

class User(object):
  def __init__(self, uuid):
    self.uuid = uuid
    # TODO: Read this from a file instead
    self.defaultSettings = {
      'result_url': 'https://e-mission.eecs.berkeley.edu/compare'
    }

  @staticmethod
  def isRegistered(userEmail):
    email2UUID = get_uuid_db().find_one({'user_email': userEmail})
    if email2UUID is None:
      return False
    else:
      return True

  @staticmethod
  def fromEmail(userEmail):
    email2UUID = get_uuid_db().find_one({'user_email': userEmail})
    if email2UUID is None:
      return None
    user = User(email2UUID['uuid'])
    user.__email = userEmail
    return user

  @staticmethod
  def fromUUID(user_uuid):
    user = User(user_uuid)
    uuid2Email = get_uuid_db().find_one({'uuid': user_uuid})
    # Remove once we remove obsolete code/tests that doesn't create an email ->
    # uuid mapping
    if uuid2Email is not None and 'user_email' in uuid2Email:
      user.__email = uuid2Email['user_email']
    return user

  def getProfile(self):
    return get_profile_db().find_one({'user_id': self.uuid})

  # Returns Average of MPG of all cars the user drives
  def getAvgMpg(self):
    mpg_array = [defaultMpg]
    # All existing profiles will be missing the 'mpg_array' field.
    # TODO: Might want to write a support script here to populate it for existing data
    # and remove this additional check
    if self.getProfile() != None and 'mpg_array' in self.getProfile():
        mpg_array = self.getProfile()['mpg_array']
    total = 0
    for mpg in mpg_array:
      total += mpg
    avg = total / len(mpg_array)
    print("Returning total = %s, len = %s, avg = %s" % (total, len(mpg_array), avg))
    return avg

  # Stores Array of MPGs of all the cars the user drives.
  # At this point, guaranteed that user has a profile.
  def setMpgArray(self, mpg_array):
    logging.debug("Setting MPG array for user %s to : %s" % (self.uuid, mpg_array))
    get_profile_db().update_one({'user_id': self.uuid}, {'$set': {'mpg_array': mpg_array}})

  def update(self, update_doc):
    logging.debug("Updating user %s with fields %s" % (self.uuid, update_doc))
    get_profile_db().update_one({'user_id': self.uuid}, {'$set': update_doc})

  def getCarbonFootprintForMode(self):
    logging.debug("Setting Carbon Footprint map for user %s to" % (self.uuid))
    #using conversion: 8.91 kg CO2 for one gallon
    #must convert Mpg -> Km, factor of 1000 in denom for g -> kg conversion
    avgMetersPerGallon = self.getAvgMpg()*1.6093
    car_footprint = (1 / avgMetersPerGallon) * 8.91
    modeMap = {'walking' : 0,
                          'running' : 0,
                          'cycling' : 0,
                            'mixed' : 0,
                        'bus_short' : 267.0 / 1609,
                         'bus_long' : 267.0 / 1609,
                       'train_short' : 92.0 / 1609,
                        'train_long' : 92.0 / 1609,
                        'car_short' : car_footprint,
                         'car_long' : car_footprint,
                        'air_short' : 217.0 / 1609,
                         'air_long' : 217.0 / 1609
                      }
    return modeMap

  def getUpdateTS(self):
    return self.getProfile()['update_ts']

  def changeUpdateTs(self, timedelta):
    newTs = self.getUpdateTS() + timedelta
    get_profile_db().update_one({'user_id': self.uuid}, {'$set': {'update_ts': newTs}})

  def getFirstStudy(self):
    return None

  @staticmethod
  def mergeDicts(dict1, dict2):
    retDict = dict1.copy()
    retDict.update(dict2)
    return retDict

  def getSettings(self):
    from emission.core.wrapper.client import Client

    # Combine profile settings and study settings.
    # We currently don't have any profile settings
    retSettings = self.defaultSettings;
    logging.debug("To return user settings, using defaults")
    return retSettings

  @staticmethod
  def _createInitialProfile(uuid):
    now = datetime.now()
    initProfileObj = {
                      'user_id': uuid,
                      'source':'Shankari',
                      'create_ts': now,
                      'update_ts': now,
                      'mpg_array': [defaultMpg],
                      'mode': {},
                      'purpose': {}
                    }
    return get_profile_db().insert_one(initProfileObj)

  @staticmethod
  def _createInitialMapping(userEmail):
    import uuid
    now = datetime.now()
    emailUUIDObject = {'user_email': userEmail, 'uuid': uuid.uuid4(), "create_ts": now, 'update_ts': now}
    get_uuid_db().insert_one(emailUUIDObject)
    return emailUUIDObject["uuid"]

  # Create assumes that we will definitely create a new one every time.
  # This introduces state and makes things complex.
  # Instead, we can write an idempotent function that will create if necessary
  # Because of that, I guess that we call this "update"
  # Returns the study that the user is part of, if any, or None if the user is
  # not part of a study
  @staticmethod
  def register(userEmail):
    # we create entries in two databases when we register:
    # `uuid_db`: which has the mapping between the token and the UUID
    # `profile_db`: which only has the UUID but a bunch of other information
    # related to the user, such as the phone make/model, and summaries about
    # number of trips, last trip, last call, las push, etc
    # Since we make two DB calls, we can end up with an inconsistency where the
    # UUID is created but not the profile. If we detect that, we will try to
    # create the profile anyway.
    userEmailQuery = {'user_email': userEmail}
    existing_mapping = get_uuid_db().find_one(userEmailQuery)
    if existing_mapping is None:
        uuid = User._createInitialMapping(userEmail)
    else:
        uuid = existing_mapping['uuid']
        get_uuid_db().update_one(userEmailQuery, {"$set": {"update_ts": datetime.now()}})

    useridQuery = {"user_id": uuid}
    existing_profile = get_profile_db().find_one(useridQuery)
    if existing_profile is None:
        profile_id = User._createInitialProfile(uuid)
    else:
        profile_id = existing_profile["_id"]
        get_profile_db().update_one(useridQuery, {"$set": {"update_ts": datetime.now()}})

    return User.fromUUID(uuid)

  @staticmethod
  def unregister(userEmail):
    user = User.fromEmail(userEmail)
    uuid = user.uuid
    get_uuid_db().delete_one({'user_email': userEmail})
    get_profile_db().delete_one({'user_id': uuid})
    return uuid

  @staticmethod
  def generate_username():
      words_by_first_char = _load_words()
      rng = random.SystemRandom()

      first_char, second_char = rng.sample(list(words_by_first_char.keys()), 2)
      first_word = rng.choice(words_by_first_char[first_char])
      second_word = rng.choice(words_by_first_char[second_char])
      return f"{first_word}_{second_word}"

  def create_and_store_username(self):
      user = get_profile_db().find_one({'user_id': self.uuid})
      if 'username' in user and user['username']:
          return user['username']
      generated_username = User.generate_username()
      logging.info(f"Generated username for user {self.uuid}: {generated_username}")
      get_profile_db().update_one({'user_id': self.uuid}, {'$set': {'username': generated_username}})
      return generated_username

  def getUserCustomLabel(self, key):
    user = get_profile_db().find_one({'user_id': self.uuid})
    if key in user:
      labels = user[key]
      filteredLabels = {key: value for key, value in labels.items() if value.get('isActive', False)}
      sortedLabels = dict(sorted(filteredLabels.items(), key=lambda x: (x[1]["frequency"]), reverse=True))
      return list(sortedLabels)  
    else:
      return []

  def insertUserCustomLabel(self, inserted_label):
    from datetime import datetime
    user = get_profile_db().find_one({'user_id': self.uuid})
    key = inserted_label['key']
    label = inserted_label['label']
    items = user[key] if key in user else {} 
    
    # if label exists in database, chage it as 'active' label
    if label in items:
      items[label]['isActive'] = True
    else:
      items[label] = {
        'createdAt': datetime.now(),
        'frequency': 0,
        'isActive': True,
      }

    get_profile_db().update_one({'user_id': self.uuid}, {'$set': {key: items}})
    return self.getUserCustomLabel(key)
  
  def updateUserCustomLabel(self, updated_label):
    from datetime import datetime
    user = get_profile_db().find_one({'user_id': self.uuid})
    key = updated_label['key']
    items = user[key] if key in user else {} 
    old_label = updated_label['old_label']
    new_label = updated_label['new_label']
    is_new_label_must_added = updated_label['is_new_label_must_added']
    # when a user changed a label to an exsiting customized label
    if new_label in items:
      updated_frequency = items[new_label]['frequency'] + 1
      items[new_label]['frequency'] = updated_frequency
      items[new_label]['isActive'] = True
    
    # when a user added a new customized label
    if is_new_label_must_added and not new_label in items:
      items[new_label] = {
        'createdAt': datetime.now(),
        'frequency': 1,
        'isActive': True,
      }

    # when a user chaged a label from an exsiting customized label
    if old_label in items:
      updated_frequency = items[old_label]['frequency'] - 1
      items[old_label]['frequency'] = updated_frequency

    get_profile_db().update_one({'user_id': self.uuid}, {'$set': {key: items}})
    return self.getUserCustomLabel(key)
  
  def deleteUserCustomLabel(self, deleted_label):
    user = get_profile_db().find_one({'user_id': self.uuid})
    key = deleted_label['key']
    label = deleted_label['label']
    items = user[key] if key in user else {} 

    if label in items:
      items[label]['isActive'] = False

    get_profile_db().update_one({'user_id': self.uuid}, {'$set': {key: items}})
    return self.getUserCustomLabel(key)
