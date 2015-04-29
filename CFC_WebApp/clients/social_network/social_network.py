import datetime
import json
from bottle import request
import facebook
from clients.default import default
from dao.user import User
from get_database import get_facebook_db, get_uuid_db
from main import get_database
from open_facebook.api import OpenFacebook
"""
Instructions:

    - Get key values from Sid and `cp settings.json.sample settings.json`
"""

settings_file = open('settings.json')
key_data = json.load(settings_file)
private_key = key_data["fb_app_secret"]
client_key = key_data["fb_app_id"]



class FacebookUser(User):
    """
    Defines a new type User object to take advantage of existing User infrastructure.
    """
    def __init__(self, uuid, access_token):
        self.access_token = access_token
        User.__init__(self, uuid)

    def registerFbProfile(self):
        graph = OpenFacebook(self.access_token)
        user_data = graph.get('me')
        fb_db = get_facebook_db()

        fb_prof_object = {'user_id':self.uuid,
                          'facebook_id':user_data['id'],
                          'fb_email':user_data['email'],
                          'image_url':graph.my_image_url()}

        return fb_db.update(
            {'user_id':self.uuid},
              {'$set':{'user_profile':fb_prof_object}},upsert=True)


    def getFriends(self):
        pass

# These are copy/pasted from our first client, the carshare study
def getSectionFilter(uuid):
    # We are not planning to do any filtering for this study. Bring on the worst!
    return []


def clientSpecificSetters(uuid, sectionId, predictedModeMap):
    return None


def getClientConfirmedModeField():
    return None


# TODO: Simplify this. runBackgroundTasks are currently only invoked from the
# result precomputation code. We could change that code to pass in the day, and
# remove this interface. Extra credit: should we pass in the day, or a date
# range?  Passing in the date range could make it possible for us to run the
# scripts more than once a day...
def runBackgroundTasks(uuid):
    today = datetime.now().date()
    runBackgroundTasksForDay(uuid, today)


def runBackgroundTasksForDay(uuid, today):
    default.runBackgroundTasksForDay(uuid, today)
