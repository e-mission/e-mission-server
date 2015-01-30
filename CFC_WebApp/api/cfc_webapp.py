import json
from random import randrange
from bottle import route, post, get, run, template, static_file, request, app, HTTPError, SimpleTemplate, abort
# import import_my_lib
# To support dynamic loading of client-specific libraries
import sys
import os
import logging
from datetime import datetime
# So that we can set the socket timeout
import socket
# For decoding tokens using the google decode URL
# We want to switch this to something offline later
import urllib
import requests

config_file = open('config.json')
config_data = json.load(config_file)
static_path = config_data["paths"]["static_path"]
python_path = config_data["paths"]["python_path"]
server_host = config_data["server"]["host"]
server_port = config_data["server"]["port"]
socket_timeout = config_data["server"]["timeout"]

key_file = open('keys.json')
key_data = json.load(key_file)
ssl_cert = key_data["ssl_certificate"]
private_key = key_data["private_key"]
client_key = key_data["client_key"]
hack_client_key = key_data["ios_client_key"]

skipAuth = False

app = app()

# On MacOS, the current working directory is always in the python path However,
# on ubuntu, it looks like the script directory (api in our case) is in the
# python path, but the pwd is not. This means that "main" is not seen even if
# we run from the CFC_WebApp directory. Let's make sure to manually add it to
# the python path so that we can keep our separation between the main code and
# the webapp layer
print("old path is %s" % sys.path)
sys.path.append(os.getcwd())
print("new path is %s" % sys.path)

logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s',
                  filename=config_data["paths"]["log_file"], level=logging.DEBUG)

from main import modeshare, zipcode, distance, tripManager, auth,\
                 carbon, commute, work_time, Berkeley, common, visualize, stats
from dao.client import Client
from dao.user import User
from get_database import get_uuid_db, get_mode_db

#Simple path that serves up a static landing page with javascript in it
@route('/')
def index():
  return static_file("index.html", static_path)

# Bunch of static pages that constitute our website
# Should we have gone for something like django instead after all?
# If this gets to be too much, we should definitely consider that
@route("/<filename>")
def doc(filename):
  if filename != "privacy" and filename != "support" and filename != "about" and filename != "consent":
    return HTTPError(404, "Don't try to hack me, you evil spammer")
  else:
    return static_file("%s.html" % filename, "%s/docs/" % static_path)

# Serve up javascript and css files properly
@route('/front/<filename>')
def server_static(filename):
  return static_file(filename, static_path)

# Returns the proportion of survey takers who use each mode
@route('/result/commute.modeshare.distance')
def getCommuteModeShare():
  fromTs = request.query.from_ts
  toTs = request.query.to_ts
  logging.debug("Filtering values for range %s -> %s" % (fromTs, toTs))
  return modeshare.get_Alluser_mode_share_by_distance("commute",
    datetime.fromtimestamp(float(fromTs)/1000), datetime.fromtimestamp(float(toTs)/1000))
  # return modeshare.getModeShare()

@route('/result/internal.modeshare.distance')
def getBerkeleyModeShare():
  fromTs = request.query.from_ts
  toTs = request.query.to_ts
  logging.debug("Filtering values for range %s -> %s" % (fromTs, toTs))
  return Berkeley.get_berkeley_mode_share_by_distance(
    datetime.fromtimestamp(float(fromTs)/1000), datetime.fromtimestamp(float(toTs)/1000))
  # return modeshare.getModeShare()

# Returns the modeshare by zipcode
@route('/result/commute.modeshare/zipcode/<zc>')
def getCommuteModeShare(zc):
  fromTs = request.query.from_ts
  toTs = request.query.to_ts
  logging.debug("Filtering values for range %s -> %s" % (fromTs, toTs))
  return zipcode.get_mode_share_by_Zipcode(zc, "commute",
    datetime.fromtimestamp(float(fromTs)/1000), datetime.fromtimestamp(float(toTs)/1000))

# Returns the proportion of survey takers from different zip codes
@route('/result/home.zipcode')
def getZipcode():
  return zipcode.getZipcode()

# Returns the proportion of commute distances
@route('/result/commute.distance.to')
def getDistance():
  fromTs = request.query.from_ts
  toTs = request.query.to_ts
  logging.debug("Filtering values for range %s -> %s" % (fromTs, toTs))
  distances = distance.get_morning_commute_distance_pie(
    datetime.fromtimestamp(float(fromTs)/1000), datetime.fromtimestamp(float(toTs)/1000))
  # logging.debug("Returning distances = %s" % distances)
  return distances

@route('/result/commute.distance.from')
def getDistance():
  fromTs = request.query.from_ts
  toTs = request.query.to_ts
  logging.debug("Filtering values for range %s -> %s" % (fromTs, toTs))
  distances = distance.get_evening_commute_distance_pie(
    datetime.fromtimestamp(float(fromTs)/1000), datetime.fromtimestamp(float(toTs)/1000))
  # logging.debug("Returning distances = %s" % distances)
  return distances

# Returns the distribution of commute arrival and departure times
@route('/result/commute.arrivalTime')
def getArrivalTime():
  fromTs = request.query.from_ts
  toTs = request.query.to_ts
  logging.debug("Filtering values for range %s -> %s" % (fromTs, toTs))
  retVal = work_time.get_Alluser_work_start_time_pie(
    datetime.fromtimestamp(float(fromTs)/1000), datetime.fromtimestamp(float(toTs)/1000))
  # retVal = common.generateRandomResult(['00-04', '04-08', '08-10'])
  # logging.debug("In getArrivalTime, retVal is %s" % retVal)
  return retVal

@route('/result/commute.departureTime')
def getDepartureTime():
  fromTs = request.query.from_ts
  toTs = request.query.to_ts
  logging.debug("Filtering values for range %s -> %s" % (fromTs, toTs))
  retVal = work_time.get_Alluser_work_end_time_pie(
    datetime.fromtimestamp(float(fromTs)/1000), datetime.fromtimestamp(float(toTs)/1000))
  # retVal = common.generateRandomResult(['00-04', '04-08', '08-10'])
  # logging.debug("In getDepartureTime, retVal is %s" % retVal)
  return retVal

@route("/result/heatmap/carbon")
def getCarbonHeatmap():
  fromTs = request.query.from_ts
  toTs = request.query.to_ts
  logging.debug("Filtering values for range %s -> %s" % (fromTs, toTs))
  retVal = visualize.carbon_by_zip(
    datetime.fromtimestamp(float(fromTs)/1000), datetime.fromtimestamp(float(toTs)/1000))
  # retVal = common.generateRandomResult(['00-04', '04-08', '08-10'])
  # logging.debug("In getCarbonHeatmap, retVal is %s" % retVal)
  return retVal

@route("/result/heatmap/pop.route/cal")
def getCalPopRoute():
  fromTs = request.query.from_ts
  toTs = request.query.to_ts
  logging.debug("Filtering values for range %s -> %s" % (fromTs, toTs))
  retVal = visualize.Berkeley_pop_route(
    datetime.fromtimestamp(float(fromTs)/1000), datetime.fromtimestamp(float(toTs)/1000))
  # retVal = common.generateRandomResult(['00-04', '04-08', '08-10'])
  # logging.debug("In getCalPopRoute, retVal is %s" % retVal)
  return retVal

@route("/result/heatmap/pop.route/commute/<selMode>")
def getCommutePopRoute(selMode):
  mode = get_mode_db().find_one({'mode_name': selMode})
  fromTs = request.query.from_ts
  toTs = request.query.to_ts
  logging.debug("Filtering values for range %s -> %s" % (fromTs, toTs))
  retVal = visualize.Commute_pop_route(mode['mode_id'],
    datetime.fromtimestamp(float(fromTs)/1000), datetime.fromtimestamp(float(toTs)/1000))
  # retVal = common.generateRandomResult(['00-04', '04-08', '08-10'])
  # logging.debug("In getCalPopRoute, retVal is %s" % retVal)
  return retVal

@get('/result/carbon/all/summary')
def carbonSummaryAllTrips():
  fromTs = request.query.from_ts
  toTs = request.query.to_ts
  logging.debug("Filtering values for range %s -> %s" % (fromTs, toTs))
  return carbon.getSummaryAllTrips(
      datetime.fromtimestamp(float(fromTs)/1000), datetime.fromtimestamp(float(toTs)/1000))

@get('/tripManager/getModeOptions')
def getModeOptions():
  return tripManager.getModeOptions()

@post('/tripManager/getUnclassifiedSections')
def getUnclassifiedSections():
  user_uuid=getUUID(request)
  return tripManager.getUnclassifiedSections(user_uuid)

@post('/tripManager/setSectionClassification')
def setSectionClassification():
  user_uuid=getUUID(request)
  updates = request.json['updates']
  return tripManager.setSectionClassification(user_uuid, updates)

@post('/profile/create')
def createUserProfile():
  logging.debug("Called createUserProfile")
  userToken = request.json['user']
  # This is the only place we should use the email, since we may not have a
  # UUID yet. All others should only use the UUID.
  userEmail = verifyUserToken(userToken)
  logging.debug("userEmail = %s" % userEmail)
  user = User.register(userEmail)
  logging.debug("Looked up user = %s" % user)
  logging.debug("Returning result %s" % {'uuid': str(user.uuid)})
  return {'uuid': str(user.uuid)}

@post('/profile/consent')
def setConsentInProfile():
  user_uuid = getUUID(request)
  version = request.json['version']
  print "Setting accepted version to %s for user %s" % (version, user_uuid)
  logging.debug("Setting accepted version to %s for user %s" % (version, user_uuid))
  return None

@post('/profile/settings')
def getCustomizationForProfile():
  user_uuid = getUUID(request)
  user = User.fromUUID(user_uuid)
  logging.debug("Returning settings for user %s" % user_uuid)
  return user.getSettings()

@post('/stats/set')
def setStats():
  user_uuid=getUUID(request)
  inStats = request.json['stats']
  stats.setClientMeasurements(user_uuid, inStats)

@post('/compare')
# @get('/compare')
def getCarbonCompare():
  if request.json == None:
    return "Waiting for user data to become available..."

  if 'user' not in request.json:
    return "Waiting for user data to be become available.."

  user_uuid = getUUID(request)
  user = User.fromUUID(user_uuid)
  if user.getFirstStudy() == 'carshare':
    return callStudy("carshare", "classifiedCount")
  
  (myModeShareCount, avgModeShareCount,
     myModeShareDistance, avgModeShareDistance,
     myModeCarbonFootprint, avgModeCarbonFootprint,
     myOptimalCarbonFootprint, avgOptimalCarbonFootprint) = carbon.getFootprintCompare(user_uuid)

  renderedTemplate = template("compare.html",
                      myModeShareCount = json.dumps(myModeShareCount),
                      avgModeShareCount = json.dumps(avgModeShareCount),
                      myModeShareDistance = json.dumps(myModeShareDistance),
                      avgModeShareDistance = json.dumps(avgModeShareDistance),
                      myModeCarbonFootprint = json.dumps(myModeCarbonFootprint),
                      avgModeCarbonFootprint = json.dumps(avgModeCarbonFootprint),
                      myOptimalCarbonFootprint = json.dumps(myOptimalCarbonFootprint),
                      avgOptimalCarbonFootprint = json.dumps(avgOptimalCarbonFootprint))
                  
  # logging.debug(renderedTemplate)
  return renderedTemplate

# Client related code START
@post("/client/<clientname>/<method>")
def callStudy(clientname, method):
  user_uuid = getUUID(request)
  request['user'] = user_uuid
  return Client(clientname).callMethod(method, request)

@get('/client/pre-register')
def registeredForStudy():
  userEmail = request.query.email
  client = request.query.client
  client_key = request.query.client_key
  logging.debug("request = %s" % (request))
  logging.debug("userEmail = %s, client = %s, client_key = %s" % (userEmail, client, client_key))
  # try:
  newSignupCount = Client(client).preRegister(client_key, userEmail)
  # except Exception as e:
  #   abort(e.code, e.msg)
  return {'email': userEmail, 'client': client, 'signup_count': newSignupCount }
# Client related code END

# Data source integration START
@post('/movesCallback')
def movesCallback():
  logging.debug("Request from user = %s" % request)
  logging.debug("Request.json from user = %s" % request.json)
  user_uuid = getUUID(request)
  if user_uuid is None:
    # Hack to support older clients that don't call register before calling movesCallback
    # Remove by Dec 31, 2014
    createUserProfile()
    user_uuid = getUUID(request)
  assert(user_uuid is not None)
  code = request.json['code']
  state = request.json['state']
  return auth.movesCallback(code, state, user_uuid)
# Data source integration END

@app.hook('before_request')
def before_request():
  print("START %s %s %s" % (datetime.now(), request.method, request.path))
  logging.debug("START %s %s" % (request.method, request.path))

@app.hook('after_request')
def after_request():
  print("END %s %s %s" % (datetime.now(), request.method, request.path))
  logging.debug("END %s %s" % (request.method, request.path))

# Auth helpers BEGIN
# This should only be used by createUserProfile since we may not have a UUID
# yet. All others should use the UUID.
def verifyUserToken(token):
  constructedURL = ("https://www.googleapis.com/oauth2/v1/tokeninfo?id_token=%s"%token)
  r = requests.get(constructedURL)
  tokenFields = json.loads(r.content)
  in_client_key = tokenFields['audience']
  if (in_client_key != client_key):
    if (in_client_key != hack_client_key):
      abort(401, "Invalid client key %s" % in_client_key)
  logging.debug("Found user email %s" % tokenFields['email'])
  return tokenFields['email']

def getUUIDFromToken(token):
    userEmail = verifyUserToken(token)
    user=User.fromEmail(userEmail)
    if user is None:
      return None
    user_uuid=user.uuid
    return user_uuid

def getUUID(request):
  if skipAuth:
    from uuid import UUID
    from get_database import get_uuid_db
    if get_uuid_db().find().count() == 1:
      user_uuid = get_uuid_db().find_one()['uuid']
    else:
      # TODO: Figure out what we really want to do here
      user_uuid = UUID('{3a307244-ecf1-3e6e-a9a7-3aaf101b40fa}')
    logging.debug("skipAuth = %s, returning fake UUID %s" % (skipAuth, user_uuid))
    return user_uuid
  else:
    userToken = request.json['user']
    return getUUIDFromToken(userToken)
# Auth helpers END

# We have see the sockets hang in practice. Let's set the socket timeout = 1
# hour to be on the safe side, and see if it is hit.
socket.setdefaulttimeout(float(socket_timeout))

# The selection of SSL versus non-SSL should really be done through a config
# option and not through editing source code, so let's make this keyed off the
# port number
if server_port == "443":
  # We support SSL and want to use it
  run(host=server_host, port=server_port, server='cherrypy', debug=True,
      certfile=ssl_cert, keyfile=private_key, ssl_module='builtin')
else:
  # Non SSL option for testing on localhost
  # We can theoretically use a separate skipAuth flag specified in the config file,
  # but then we have to define the behavior if SSL is true and we are not
  # running on localhost but still want to run without authentication. That is
  # not really an important use case now, and it makes people have to change
  # two values and increases the chance of bugs. So let's key the auth skipping from this as well.
  skipAuth = True
  print "Running with HTTPS turned OFF, skipAuth = True"

  run(host=server_host, port=server_port, server='cherrypy', debug=True)

# run(host="0.0.0.0", port=server_port, server='cherrypy', debug=True)
