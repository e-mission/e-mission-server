# Standard imports
import json
from random import randrange
from bottle import route, post, get, run, template, static_file, request, app, HTTPError, abort, BaseRequest, JSONPlugin
import bottle as bt
# To support dynamic loading of client-specific libraries
import sys
import os
import logging
logging.basicConfig(format='%(asctime)s:%(levelname)s:%(thread)d:%(message)s',
                  filename='webserver_debug.log', level=logging.DEBUG)
logging.debug("This should go to the log file")

from datetime import datetime
import time
# So that we can set the socket timeout
import socket
# For decoding JWTs using the google decode URL
import urllib
import requests
# For decoding JWTs on the client side
import oauth2client.client
from oauth2client.crypt import AppIdentityError
import traceback
import xmltodict
import urllib2
import bson.json_util

# Our imports
import modeshare, zipcode, distance, tripManager, \
                 Berkeley, visualize, stats, usercache, timeline
import emission.net.ext_service.moves.register as auth
import emission.net.ext_service.habitica.register as habitreg
import emission.analysis.result.carbon as carbon
import emission.analysis.classification.inference.commute as commute
import emission.analysis.modelling.work_time as work_time
import emission.analysis.result.userclient as userclient
import emission.core.common as common
from emission.core.wrapper.client import Client
from emission.core.wrapper.user import User
from emission.core.get_database import get_uuid_db, get_mode_db
import emission.core.wrapper.motionactivity as ecwm


config_file = open('conf/net/api/webserver.conf')
config_data = json.load(config_file)
static_path = config_data["paths"]["static_path"]
python_path = config_data["paths"]["python_path"]
server_host = config_data["server"]["host"]
server_port = config_data["server"]["port"]
socket_timeout = config_data["server"]["timeout"]
log_base_dir = config_data["paths"]["log_base_dir"]

key_file = open('conf/net/keys.json')
key_data = json.load(key_file)
ssl_cert = key_data["ssl_certificate"]
private_key = key_data["private_key"]
client_key = key_data["client_key"]
client_key_old = key_data["client_key_old"]
ios_client_key = key_data["ios_client_key"]

BaseRequest.MEMFILE_MAX = 1024 * 1024 * 1024 # Allow the request size to be 1G
# to accomodate large section sizes

skipAuth = False
print "Finished configuring logging for %s" % logging.getLogger()
app = app()

# On MacOS, the current working directory is always in the python path However,
# on ubuntu, it looks like the script directory (api in our case) is in the
# python path, but the pwd is not. This means that "main" is not seen even if
# we run from the CFC_WebApp directory. Let's make sure to manually add it to
# the python path so that we can keep our separation between the main code and
# the webapp layer

#Simple path that serves up a static landing page with javascript in it
@route('/')
def index():
  return static_file("index.html", static_path)

# Bunch of static pages that constitute our website
# Should we have gone for something like django instead after all?
# If this gets to be too much, we should definitely consider that
@route("/docs/<filename>")
def docs(filename):
  if filename != "privacy" and filename != "support" and filename != "about" and filename != "consent":
    return HTTPError(404, "Don't try to hack me, you evil spammer")
  else:
    return static_file("%s.html" % filename, "%s/%s" % (static_path, "docs"))

@route("/<filename>")
def docs(filename):
  if filename != "privacy" and filename != "support" and filename != "about" and filename != "consent":
    return HTTPError(404, "Don't try to hack me, you evil spammer")
  else:
    return static_file("%s.html" % filename, "%s/%s" % (static_path, "docs"))

# Serve up the components of the webapp - library files, our javascript and css
# files, and HTML templates, properly
@route('/css/<filepath:path>')
def server_css(filepath):
    logging.debug("static filepath = %s" % filepath)
    return static_file(filepath, "%s/%s" % (static_path, "css"))

@route('/img/<filepath:path>')
def server_img(filepath):
    logging.debug("static filepath = %s" % filepath)
    return static_file(filepath, "%s/%s" % (static_path, "img"))

@route('/js/<filepath:path>')
def server_js(filepath):
    logging.debug("static filepath = %s" % filepath)
    return static_file(filepath, "%s/%s" % (static_path, "js"))

@route('/lib/<filepath:path>')
def server_lib(filepath):
    logging.debug("static filepath = %s" % filepath)
    return static_file(filepath, "%s/%s" % (static_path, "lib"))

@route('/templates/<filepath:path>')
def server_templates(filepath):
  logging.debug("static filepath = %s" % filepath)
  return static_file(filepath, "%s/%s" % (static_path, "templates"))

@route('/clients/<clientname>/front/<filename>')
def server_static(clientname, filename):
  logging.debug("returning file %s from client %s " % (filename, clientname))
  return static_file(filename, "clients/%s/%s" % (clientname, static_path))

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

@post("/result/heatmap/pop.route")
def getPopRoute():
  modes = request.json['modes']
  from_ld = request.json['from_local_date']
  to_ld = request.json['to_local_date']
  region = request.json['sel_region']
  logging.debug("Filtering values for range %s -> %s, region %s" % 
        (from_ld, to_ld, region))
  retVal = visualize.range_mode_heatmap(modes, from_ld, to_ld, region)
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

@post('/tripManager/storeSensedTrips')
def storeSensedTrips():
  logging.debug("Called storeSensedTrips")
  user_uuid=getUUID(request)
  print "user_uuid %s" % user_uuid
  logging.debug("user_uuid %s" % user_uuid)
  sections = request.json['sections']
  return tripManager.storeSensedTrips(user_uuid, sections)

@post('/usercache/get')
def getFromCache():
  logging.debug("Called userCache.get")
  user_uuid=getUUID(request)
  logging.debug("user_uuid %s" % user_uuid)
  to_phone = usercache.sync_server_to_phone(user_uuid)
  return {'server_to_phone': to_phone}

@post('/usercache/put')
def putIntoCache():
  logging.debug("Called userCache.put")
  user_uuid=getUUID(request)
  logging.debug("user_uuid %s" % user_uuid)
  from_phone = request.json['phone_to_server']
  return usercache.sync_phone_to_server(user_uuid, from_phone)

@post('/timeline/getTrips/<day>')
def getTrips(day):
  logging.debug("Called timeline.getTrips/%s" % day)
  user_uuid=getUUID(request)
  force_refresh = request.query.get('refresh', False)
  logging.debug("user_uuid %s" % user_uuid)
  ret_geojson = timeline.get_trips_for_day(user_uuid, day, force_refresh)
  logging.debug("type(ret_geojson) = %s" % type(ret_geojson))
  ret_dict = {"timeline": ret_geojson}
  logging.debug("type(ret_dict) = %s" % type(ret_dict))
  return ret_dict

@post('/profile/create')
def createUserProfile():
  logging.debug("Called createUserProfile")
  userToken = request.json['user']
  # This is the only place we should use the email, since we may not have a
  # UUID yet. All others should only use the UUID.
  if skipAuth:
    userEmail = userToken
  else: 
    userEmail = verifyUserToken(userToken)
  logging.debug("userEmail = %s" % userEmail)
  user = User.register(userEmail)
  logging.debug("Looked up user = %s" % user)
  logging.debug("Returning result %s" % {'uuid': str(user.uuid)})
  return {'uuid': str(user.uuid)}

@post('/profile/update')
def updateUserProfile():
  logging.debug("Called updateUserProfile")
  user_uuid = getUUID(request)
  user = User.fromUUID(user_uuid)
  mpg_array = request.json['mpg_array']
  return user.setMpgArray(mpg_array)


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
def postCarbonCompare():
  from clients.data import data
  from clients.choice import choice

  if not skipAuth:
      if request.json == None:
        return "Waiting for user data to become available..."
      if 'user' not in request.json:
        return "Waiting for user data to be become available.."

  user_uuid = getUUID(request)

  clientResult = userclient.getClientSpecificResult(user_uuid)
  if clientResult != None:
    logging.debug("Found overriding client result for user %s, returning it" % user_uuid)
    return clientResult
  else:
    logging.debug("No overriding client result for user %s, returning choice " % user_uuid)
  return choice.getResult(user_uuid)

@get('/compare')
def getCarbonCompare():
  for key, val in request.headers.items():
    print("  %s: %s" % (key, val))

  from clients.data import data

  if not skipAuth:
    if 'User' not in request.headers or request.headers.get('User') == '':
        return "Waiting for user data to become available..."
  
  from clients.choice import choice

  user_uuid = getUUID(request, inHeader=True)
  print ('UUID', user_uuid)
  
  clientResult = userclient.getClientSpecificResult(user_uuid)
  if clientResult != None:
    logging.debug("Found overriding client result for user %s, returning it" % user_uuid)
    return clientResult
  else:
    logging.debug("No overriding client result for user %s, returning choice" % user_uuid)
  return choice.getResult(user_uuid)

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

@get('/client/<clientName>/<method>')
def javascriptCallback(clientName, method):
  from clients.choice import choice

  client = Client(clientName)
  client_key = request.query.client_key
  client.callJavascriptCallback(client_key, method, request.params)
  return {'status': 'ok'}

# proxy used to request and process XML from an external API, then convert it to JSON
# original URL should be encoded in UTF-8
@get("/asJSON/<originalXMLWebserviceURL>")
def xmlProxy(originalXMLWebserviceURL):
  decodedURL = urllib2.unquote(originalXMLWebserviceURL)
  f = urllib2.urlopen(decodedURL)
  xml = f.read()
  parsedXML = xmltodict.parse(xml)
  return json.dumps(parsedXML)

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

@post('/habiticaRegister')
def habiticaRegister():
  logging.debug("habitica registration request %s from user = %s" %
                (request.json, request))
  user_uuid = getUUID(request)
  assert(user_uuid is not None)
  username = request.json['regConfig']['username']
  # This is the second place we use the email, since we need to pass
  # it to habitica to complete registration. I'm not even refactoring
  # this into a method - hopefully this makes it less likely to be reused
  userToken = request.json['user']
  if skipAuth:
      userEmail = userToken
  else:
      userEmail = verifyUserToken(userToken)
  autogen_password = "autogenerate_me"
  return habitreg.habiticaRegister(username, userEmail,
                                   autogen_password, user_uuid)
# Data source integration END

@app.hook('before_request')
def before_request():
  print("START %s %s %s" % (datetime.now(), request.method, request.path))
  request.params.start_ts = time.time()
  logging.debug("START %s %s" % (request.method, request.path))

@app.hook('after_request')
def after_request():
  msTimeNow = time.time()
  duration = msTimeNow - request.params.start_ts
  print("END %s %s %s %s %s " % (datetime.now(), request.method, request.path, request.params.user_uuid, duration))
  logging.debug("END %s %s %s %s " % (request.method, request.path, request.params.user_uuid, duration))
  # Keep track of the time and duration for each call
  stats.storeServerEntry(request.params.user_uuid, "%s %s" % (request.method, request.path),
        msTimeNow, duration)

# Auth helpers BEGIN
# This should only be used by createUserProfile since we may not have a UUID
# yet. All others should use the UUID.
def verifyUserToken(token):
    try:
        # attempt to validate token on the client-side
        logging.debug("Using OAuth2Client to verify id token of length %d from android phones" % len(token))
        tokenFields = oauth2client.client.verify_id_token(token,client_key)
        logging.debug(tokenFields)
    except AppIdentityError as androidExp:
        try:
            logging.debug("Using OAuth2Client to verify id token of length %d from android phones using old token" % len(token))
            tokenFields = oauth2client.client.verify_id_token(token,client_key_old)
            logging.debug(tokenFields)
        except AppIdentityError as androidExpOld:
            try:
                logging.debug("Using OAuth2Client to verify id token from iOS phones")
                tokenFields = oauth2client.client.verify_id_token(token, ios_client_key)
                logging.debug(tokenFields)
            except AppIdentityError as iOSExp:
                traceback.print_exc()
                logging.debug("OAuth failed to verify id token, falling back to constructedURL")
                #fallback to verifying using Google API
                constructedURL = ("https://www.googleapis.com/oauth2/v1/tokeninfo?id_token=%s" % token)
                r = requests.get(constructedURL)
                tokenFields = json.loads(r.content)
                in_client_key = tokenFields['audience']
                if (in_client_key != client_key):
                    if (in_client_key != ios_client_key):
                        abort(401, "Invalid client key %s" % in_client_key)
    logging.debug("Found user email %s" % tokenFields['email'])
    return tokenFields['email']

def getUUIDFromToken(token):
    userEmail = verifyUserToken(token)
    return __getUUIDFromEmail__(userEmail)

# This should not be used for general API calls
def __getUUIDFromEmail__(userEmail):
    user=User.fromEmail(userEmail)
    if user is None:
      return None
    user_uuid=user.uuid
    return user_uuid

def __getToken__(request, inHeader):
    if inHeader:
      userHeaderSplitList = request.headers.get('User').split()
      if len(userHeaderSplitList) == 1:
          userToken = userHeaderSplitList[0]
      else:
          userToken = userHeaderSplitList[1]
    else:
      userToken = request.json['user']

    return userToken

def getUUID(request, inHeader=False):
  retUUID = None
  if skipAuth:
    if 'User' in request.headers or 'user' in request.json:
        # skipAuth = true, so the email will be sent in plaintext
        userEmail = __getToken__(request, inHeader)
        retUUID = __getUUIDFromEmail__(userEmail)
        logging.debug("skipAuth = %s, returning UUID directly from email %s" % (skipAuth, retUUID))
    else:
        # Return a random user to make it easy to experiment without having to specify a user
        # TODO: Remove this if it is not actually used
        from emission.core.get_database import get_uuid_db
        user_uuid = get_uuid_db().find_one()['uuid']
        retUUID = user_uuid
        logging.debug("skipAuth = %s, returning arbitrary UUID %s" % (skipAuth, retUUID))
    if Client("choice").getClientKey() is None:
        Client("choice").update(createKey = True)
  else:
    userToken = __getToken__(request, inHeader)
    retUUID = getUUIDFromToken(userToken)
  if retUUID is None:
     raise HTTPError(403, "token is valid, but no account found for user")
  request.params.user_uuid = retUUID
  return retUUID
# Auth helpers END

# We have see the sockets hang in practice. Let's set the socket timeout = 1
# hour to be on the safe side, and see if it is hit.
socket.setdefaulttimeout(float(socket_timeout))

for plugin in app.plugins:
    if isinstance(plugin, JSONPlugin):
        print("Replaced json_dumps in plugin with the one from bson")
        plugin.json_dumps = bson.json_util.dumps

print("Changing bt.json_loads from %s to %s" % (bt.json_loads, bson.json_util.loads))
bt.json_loads = bson.json_util.loads

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
