import json
import logging

from get_database import get_profile_db, get_pending_signup_db, get_uuid_db

defaultCarFootprint = 278.0/1609
defaultMpg = 8.91/(1.6093 * defaultCarFootprint) # Should be roughly 32

class User:
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
    return user

  def getProfile(self):
    return get_profile_db().find_one({'user_id': self.uuid})

  # In the real system, we should always have a profile for the user. But
  # during unit tests, and durig analysis, that it not always true. Let us fail
  # more gracefully if no profile is present
  def getStudy(self):
    if self.getProfile() != None:
        return self.getProfile()['study_list']
    else:
        return []

  def getFirstStudy(self):
    studyList = self.getStudy()
    if studyList == None or len(studyList) == 0:
      return None
    else:
      assert(len(studyList) == 1)
      return studyList[0]

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
    avg = total/len(mpg_array)
    print "Returning total = %s, len = %s, avg = %s" % (total, len(mpg_array), avg)
    return avg

  # Stores Array of MPGs of all the cars the user drives.
  # At this point, guaranteed that user has a profile.
  def setMpgArray(self, mpg_array):
    logging.debug("Setting MPG array for user %s to : %s" % (self.uuid, mpg_array))
    get_profile_db().update({'user_id': self.uuid}, {'$set': {'mpg_array': mpg_array}})

  def getCarbonFootprintForMode(self):
    logging.debug("Setting Carbon Footprint map for user %s to" % (self.uuid))
    #using conversion: 8.91 kg CO2 for one gallon
    #must convert Mpg -> Km, factor of 1000 in denom for g -> kg conversion
    avgMetersPerGallon = self.getAvgMpg()*1.6093
    car_footprint = (1/avgMetersPerGallon)*8.91
    modeMap = {'walking' : 0,
                          'running' : 0,
                          'cycling' : 0,
                            'mixed' : 0,
                        'bus_short' : 267.0/1609,
                         'bus_long' : 267.0/1609,
                      'train_short' : 92.0/1609,
                       'train_long' : 92.0/1609,
                        'car_short' : car_footprint,
                         'car_long' : car_footprint,
                        'air_short' : 217.0/1609,
                         'air_long' : 217.0/1609
                      }
    return modeMap

  def getUpdateTS(self):
    return self.getProfile()['update_ts']

  def changeUpdateTs(self, timedelta):
    newTs = self.getUpdateTS() + timedelta
    get_profile_db().update({'user_id': self.uuid}, {'$set': {'update_ts': newTs}})

  def setClientSpecificProfileFields(self, setQuery):
    logging.debug("Changing profile for user %s to %s" % (self.uuid, setQuery))
    get_profile_db().update({'user_id': self.uuid}, {'$set': setQuery})

  @staticmethod
  def mergeDicts(dict1, dict2):
    retDict = dict1.copy()
    retDict.update(dict2)
    return retDict

  def getSettings(self):
    from dao.client import Client

    # Combine profile settings and study settings.
    # We currently don't have any profile settings
    retSettings = self.defaultSettings;
    studyList = self.getStudy()
    if len(studyList) > 0:
      logging.debug("To return user settings, combining %s data from %s" % (Client(studyList[0]).getSettings(), studyList[0]))
      retSettings = User.mergeDicts(retSettings, Client(studyList[0]).getSettings())
      logging.debug("After merge retSettings = %s" % retSettings)
    else:
      logging.debug("To return user settings, using defaults")
    return retSettings

  def setStudy(self, study):
    # Here's what we want to do:
    # - if there is no profile entry, insert one with this uuid and the study in the study_list
    # - if there is a profile entry and the study_list does not contain this study, add it
    # - if there is a profile entry and the study_list contains the entry, keep it
    # The following mongodb statement is supposed to handle all of those cases correctly :)
    writeResult = get_profile_db().update({'user_id': self.uuid},
      {'$set': {'study_list': [study]}}, upsert=True)
    if 'err' in writeResult and writeResult['err'] is not None:
      logging.error("In setStudy, err = %s" % writeResult['err'])
      raise Exception()

  def unsetStudy(self, study):
    # Here's what we want to do:
    # - if there is no profile entry, ignore
    # - if there is a profile entry and the study_list does not contain this study, ignore
    # - if there is a profile entry and the study_list contains the entry, remove it
    # The following mongodb statement is supposed to handle all of those cases correctly :)
    writeResult = get_profile_db().update({'user_id': self.uuid},
      {'$pullAll': {'study_list': [study]}})
    if 'err' in writeResult and writeResult['err'] is not None:
      logging.error("In setStudy, err = %s" % writeResult['err'])
      raise Exception()

  @staticmethod
  def createProfile(uuid, ts, studyList):
    initProfileObj = {'user_id': uuid,
                      'source':'Shankari',
                      'update_ts': ts,
                      'mpg_array': [defaultMpg]}
    writeResultProfile = get_profile_db().update(
        {'user_id': uuid},
        {'$set': initProfileObj,
         '$addToSet': {'study_list': {'$each': studyList}}},
        upsert=True)
    return writeResultProfile

  # Create assumes that we will definitely create a new one every time.
  # This introduces state and makes things complex.
  # Instead, we can write an idempotent function that will create if necessary
  # Because of that, I guess that we call this "update"
  # Returns the study that the user is part of, if any, or None if the user is
  # not part of a study
  @staticmethod
  def register(userEmail):
    import uuid
    from datetime import datetime
    from dao.client import Client

    # We are accessing three databases here:
    # - The list of pending registrations (people who have filled out demographic
    # information but not installed the app)
    # - The mapping from the userEmail to the user UUID
    # - The mapping from the UUID to other profile information about the user
    # The first two are indexed by the user email. We will use the same field
    # name in both to indicate that it is a shared key. This also allows us to
    # have a simple query that we can reuse.
    userEmailQuery = {'user_email': userEmail}

    # First, we construct the email -> uuid mapping and store it in the appropriate database.
    # At this point, we don't know or care whether the user is part of a study
    # We also store a create timestamp just because that's always a good idea
    # What happens if the user calls register() again? Do we want to generate a new UUID?
    # Do we want to update the create timestamp?
    # For now, let's assume that the answer to both of those questions is yes,
    # because that allows us to use upsert :)
    # A bonus fix is that if something is messed up in the DB, calling create again will fix it.

    # This is the UUID that will be stored in the trip database
    # in order to do some fig leaf of anonymity
    # If we have an existing entry, should we change the UUID or not? If we
    # change the UUID, then there will be a break in the trip history. Let's
    # change for now since it makes the math easier.
    anonUUID = uuid.uuid3(uuid.NAMESPACE_URL, "mailto:%s" % userEmail.encode("UTF-8"))
    emailUUIDObject = {'user_email': userEmail, 'uuid': anonUUID, 'update_ts': datetime.now()}
    writeResultMap = get_uuid_db().update(userEmailQuery, emailUUIDObject, upsert=True)
    # Note, if we did want the create_ts to not be overwritten, we can use the
    # writeResult to decide how to deal with the values

    # Now, we look to see if the user is part of a study. We can either store
    # this information in the profile database, or the mapping, or both. For now,
    # let us store this in the profile database since it is sufficient for it to
    # be associated with the UUID, we anticipate using it for customization, and
    # we assume that other customization stuff will be stored in the profile.

    # We could also assume that we will create the profile if we created the map
    # and update if we updated. But that has some reliability issues. For
    # example, what if creating the map succeeded but creating the profile
    # failed? Subsequently calling the method again to try and fix the profile
    # will continue to fail because we will be trying to update.
    # Much better to deal with it separately by doing a separate upsert

    # Second decision: what do we do if the user is not part of a study? Create a
    # profile anyway with an empty list, or defer the creation of the profile?
    # 
    # Decision: create profile with empty list for two reasons:
    # a) for most of the functions, we want to use the profile data. We should
    # only use the email -> uuid map in the API layer to get the UUID, and use
    # the UUID elsewhere. So we need to have profiles for non-study participants
    # as well.
    # b) it will also make the scripts to update the profile in the background
    # easier to write. They won't have to query the email -> UUID database and
    # create the profile if it doesn't exist - they can just work off the profile
    # database.
    # TODO: Write a script that periodically goes through and identifies maps
    # that don't have an associated profile and fix them
    study_list = Client.getPendingClientRegs(userEmail)
    writeResultProfile = User.createProfile(anonUUID, datetime.now(), study_list)
     
    if 'err' not in writeResultProfile:
      # update was successful!
      # Either upserted or updatedExisting will be true
      # We can now cleanup the entry from the pending database
      # Note that we could also move this to a separate cleanup script because
      # eventual consistency is good enough for us
      # If there is a profile entry for a particular signup, then delete it
      Client.deletePendingClientRegs(userEmail)
    return User.fromUUID(anonUUID)

  @staticmethod
  def unregister(userEmail):
    user = User.fromEmail(userEmail)
    uuid = user.uuid
    get_uuid_db().remove({'user_email': userEmail})
    get_profile_db().remove({'user_id': uuid})
    return uuid
