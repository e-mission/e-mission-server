"""
Client module to be used as test gateway for social media authorization

Instructions:

    - Get key values from Sid and `cp settings.json.sample settings.json`

"""
import datetime
import json
import logging
from os.path import dirname



FB_BASE_DIR = dirname(dirname(__file__))

def getResult(user):
    datey = str(datetime.datetime.today())
    if user:
        from bottle import template
        return template("clients/socialmedia/result_template.html",datey=json.dumps(datey))


def registerUser(access_token):
    from dao.SocialMediaUser import FacebookUser
    # this is
    logging.info("social_media.registerUser called for user %s" % access_token)
    try:
        fb = FacebookUser.fromAccessToken(access_token)

    except:
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
    # default.runBackgroundTasksForDay(uuid, today)
    pass
