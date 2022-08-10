from __future__ import print_function
from __future__ import division
from __future__ import unicode_literals
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
from builtins import object
from past.utils import old_div
import json
import logging
import emission.storage.timeseries.abstract_timeseries as esta
import emission.storage.decorations.trip_queries as esdt
import emission.storage.timeseries.timequery as estt
import emission.core.wrapper.motionactivity as ecwm
import arrow
import emission.core.get_database as db

# Our imports
from emission.core.get_database import get_profile_db, get_uuid_db

defaultCarFootprint = old_div(278.0,1609)
defaultMpg = old_div(8.91,(1.6093 * defaultCarFootprint)) # Should be roughly 32

class User(object):
  def __init__(self, uuid):
    self.uuid = uuid
    self.username = None
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
    avg = old_div(total,len(mpg_array))
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

  #CO2 Consumption

  #def getCarbonWeek(self, n) : gets this weeks average carbon emission, where n is the nth week in the past
  #def getCarbonWeekly(self, n) : gets delta carbon emission for the nth week in the past. = getCarbonWeek(n) - getCarbonWeek(n+1)

  #def getFavTransport(self) : gets mode of transportation that is highest used... or let client handle this?
  #def getTotalDistance(self) : gets total distance travelled by user
  #def getTier(self)

  def getCarbonFootprintForMode(self):
    logging.debug("Setting Carbon Footprint map for user %s to" % (self.uuid))
    #using conversion: 8.91 kg CO2 for one gallon
    #must convert Mpg -> Km, factor of 1000 in denom for g -> kg conversion
    avgMetersPerGallon = self.getAvgMpg()*1.6093
    car_footprint = (old_div(1,avgMetersPerGallon))*8.91
    modeMap = {'walking' : 0,
                          'running' : 0,
                          'cycling' : 0,
                            'mixed' : 0,
                        'bus_short' : old_div(267.0,1609),
                         'bus_long' : old_div(267.0,1609),
                      'train_short' : old_div(92.0,1609),
                       'train_long' : old_div(92.0,1609),
                        'car_short' : car_footprint,
                         'car_long' : car_footprint,
                        'air_short' : old_div(217.0,1609),
                         'air_long' : old_div(217.0,1609)
                      }
    return modeMap

  @staticmethod
  def setUsername(user_id, username):
    """
    Sets this user's username to the inputted value
    """
    userCollection = db.get_username_db()
    userDict = userCollection.find_one({'user_id' : user_id})
    if userDict == None:
      userCollection.insert_one({'user_id' : user_id, 'username' : username})
      logging.debug('userDict  did not exist when trying to enter username')
    else:
      logging.debug('updated username to: %s' %username)
      userCollection.update_one(
        {"user_id" : user_id},
        {'$set' : {"username" : username}}
      )

  @staticmethod
  def getUsername(user_id):
    userCollection = db.get_username_db()
    userDict = userCollection.find_one({'user_id' : user_id})
    if not userDict:
      return None
    return {'username': userDict['username']}

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
  def createProfile(uuid, ts):
    initProfileObj = {'user_id': uuid,
                      'source':'Shankari',
                      'update_ts': ts,
                      'mpg_array': [defaultMpg]}
    writeResultProfile = get_profile_db().update_one(
        {'user_id': uuid},
        {'$set': initProfileObj},
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
    # This is the UUID that will be stored in the trip database
    # in order to do some fig leaf of anonymity
    # Since we now generate truly anonymized UUIDs, and we expect that the
    # register operation is idempotent, we need to check and ensure that we don't
    # change the UUID if it already exists.
    existing_entry = get_uuid_db().find_one({"user_email": userEmail})
    if existing_entry is None:
        anonUUID = uuid.uuid4()
    else:
        anonUUID = existing_entry['uuid']
    return User.registerWithUUID(userEmail, anonUUID)

  @staticmethod
  def registerWithUUID(userEmail, anonUUID):
    from datetime import datetime
    from emission.core.wrapper.client import Client

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


    emailUUIDObject = {'user_email': userEmail, 'uuid': anonUUID, 'update_ts': datetime.now()}
    writeResultMap = get_uuid_db().replace_one(userEmailQuery, emailUUIDObject, upsert=True)
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
    writeResultProfile = User.createProfile(anonUUID, datetime.now())
    return User.fromUUID(anonUUID)

  @staticmethod
  def unregister(userEmail):
    user = User.fromEmail(userEmail)
    uuid = user.uuid
    get_uuid_db().delete_one({'user_email': userEmail})
    get_profile_db().delete_one({'user_id': uuid})
    return uuid

  @staticmethod
  def carbonYesterday(user_id):
    """ Returns the previous day's carbon usage
          Calculates the sum of the footprint and penalty values
    """
    curr_ts = arrow.utcnow().timestamp
    last_ts = arrow.utcnow().shift(days = -1).timestamp
    return User.computeCarbon(user_id, last_ts, curr_ts)

  @staticmethod
  def carbonLastWeek(user_id):
    """
    Returns the daily average carbon of last week
    """
    curr_ts = arrow.utcnow().timestamp
    last_ts = arrow.utcnow().shift(weeks=-1).timestamp
    carbonMetric = User.computeCarbon(user_id, last_ts, curr_ts)
    if carbonMetric == None:
      return None
    return User.computeCarbon(user_id, last_ts, curr_ts) / 7

  @staticmethod
  def computeHappiness(user_id):
    """
    Computes the happiness of the Polar Bear
    Happiness is based on change from last week's to yesterday's
      Carbon metric.
    Ranges for moods are, on a (<) 0-1 (<) scale:
      Happy: > 0.6
      Neutral: 0.4 - 0.6
      Sad: < 0.4
    """
    carbonY = User.carbonYesterday(user_id)
    #Something is wrong with carbonLastWeek rn
    carbonLW = User.computeCarbon(user_id, arrow.utcnow().shift(weeks=-1).timestamp, arrow.utcnow().timestamp)
    if carbonLW == 0:
        if carbonY == None or carbonY <= 0.03:
            return 0.5
        else:
            return -100

    if (carbonY == None or carbonLW == None):
        return 0.5
    deltaCarbon = (carbonLW - carbonY) / (carbonLW)
    return deltaCarbon + 0.5

  @staticmethod
  def computeConfirmed(user_id, time_query=None):
    """
    Computers carbon metric for specified user.
    Formula is (Actual CO2 + penalty) / distance travelled
    """
    ts = esta.TimeSeries.get_time_series(user_id)

    ct_df = ts.get_data_df("analysis/confirmed_trip", time_query)
    if ct_df.shape[0] <= 0:
      return (0, None, None)
    ct_df_confirmed = esdt.has_final_labels_df(ct_df)
    confirmed_pct = (ct_df_confirmed.shape[0] * 100)/ ct_df.shape[0]
    if ct_df_confirmed.shape[0] <= 0:
        valid_replacement_pct = None
        score = None
    else:
        invalid_replacement = ct_df_confirmed.user_input.apply(lambda ui: ("mode_confirm" in ui and ui["mode_confirm"] == "pilot_ebike") and ("replaced_mode" not in ui or ui["replaced_mode"] == "pilot_ebike" or ui["replaced_mode"] == "same_mode"))
        invalid_replacement_freq = invalid_replacement.value_counts()
        print(invalid_replacement_freq)
        if invalid_replacement_freq.count() == 1:
            # all true or all false
            if True in invalid_replacement_freq:
                valid_replacement_pct = 0
            else:
                valid_replacement_pct = 100
        else:
            valid_replacement_pct = (invalid_replacement_freq[False] * 100)/ invalid_replacement.count()
        # multiply the two pcts to get an overall "probability"
        # but since they are percents instead of ratios, need to divide by 100
        score = (confirmed_pct * valid_replacement_pct) / 100
    return (confirmed_pct, valid_replacement_pct, score)

  @staticmethod
  def computeCarbon(user_id, last_ts, curr_ts):
    """
    Computers carbon metric for specified user.
    Formula is (Actual CO2 + penalty) / distance travelled
    """
    ts = esta.TimeSeries.get_time_series(user_id)

    last_period_tq = estt.TimeQuery("data.start_ts",
                        last_ts, # start of range
                        curr_ts)  # end of range
    cs_df = ts.get_data_df("analysis/inferred_section", time_query=last_period_tq)
    if cs_df.shape[0] <= 0:
      return None
    carbon_val = User.computeFootprint(cs_df[["sensed_mode", "distance"]])
    dist_travelled = cs_df["distance"].sum()

    if dist_travelled > 0:
      return carbon_val / dist_travelled
    # Do not include no distance traveled users in the tier system.
    return None

  @staticmethod
  def computeCarbonRaw(user_id, last_ts, curr_ts):
      """
      Computers raw carbon metric for specified user.
      Returns a tuple with noramlized and actual carbon without
      taking into account the penalty metric at all
      """
      ts = esta.TimeSeries.get_time_series(user_id)

      last_period_tq = estt.TimeQuery("data.start_ts",
                          last_ts, # start of range
                          curr_ts)  # end of range
      cs_df = ts.get_data_df("analysis/cleaned_section", time_query=last_period_tq)
      if cs_df.shape[0] <= 0:
        return None
      carbon_val = User.computeFootprint(cs_df[["sensed_mode", "distance"]])
      dist_travelled = cs_df["distance"].sum()

      if dist_travelled > 0:
        carbon_normalized = carbon_val / dist_travelled
        return [carbon_normalized, carbon_val]
      # Do not include no distance traveled users in the tier system.
      return None

  @staticmethod
  def computeFootprint(footprint_df):
    """
    Inspired by e-mission-phone/www/js/metrics-factory.js

    train: 92/1609,
    car: 287/1609,
    ON_FOOT/BICYCLING: 0

    Computes range and for now calculates the average since we
    don't distinguish between train and car.

    If unknown (sensed mode = 4), don't compute anything for now.

    footprint_df: [[trip1mode, distance], [trip2mode, distance], ...]

    """
    fp_train = 92.0/1609.0
    fp_car = 287.0/1609.0
    fp_bus = 3/4 * fp_car
    total_footprint = 0
    for index, row in footprint_df.iterrows():
        motiontype = int(row['sensed_mode'])
        distance = row['distance']
        """
        if motiontype == ecwm.MotionTypes.IN_VEHICLE.value:
            """
        if motiontype == 5: #car
            total_footprint += fp_car * User.computePenalty(motiontype, m_to_km(distance))
        elif motiontype == 4: #train
            total_footprint += fp_train * User.computePenalty(motiontype, m_to_km(distance))
        elif motiontype == 3: #bus
            total_footprint += fp_bus * User.computePenalty(motiontype, m_to_km(distance))
    return total_footprint

  @staticmethod
  def computePenalty(motiontype, distance):
    """
    Linear penalty functions are created depending on
    transportation mode
    """
    if motiontype == 5: #car
        if distance < 50:
            return distance
        return max(-(1/(2*50))*distance + 3/2, 0.5) * distance
    elif motiontype == 3: #bus
        if distance < 25:
            return distance
        return max(-(1/(2*25))*distance + 3/2, 0.5) * distance
    elif motiontype == 4: #train
        if distance < 37.5:
            return distance
        return max(-(1/(2*37.5))*distance + 3/2, 0.5) * distance





def m_to_km(distance):
    return max(0, float(distance) / float(1000))

def mil_to_km(miles):
    return float(miles) * 1609.344/float(1000)
