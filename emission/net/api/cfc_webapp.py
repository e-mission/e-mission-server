from __future__ import print_function
from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals
# Standard imports
from future import standard_library
standard_library.install_aliases()
from builtins import str
from builtins import *
from past.utils import old_div
import json
from random import randrange
from emission.net.api.bottle import route, post, get, run, template, static_file, request, app, HTTPError, abort, BaseRequest, JSONPlugin, response, error, redirect
import emission.net.api.bottle as bt
# To support dynamic loading of client-specific libraries
import sys
import os
import logging
import logging.config

from datetime import datetime
import time
import arrow
from uuid import UUID
# So that we can set the socket timeout
import socket
# For decoding JWTs using the google decode URL
import urllib.request, urllib.parse, urllib.error
import requests
import traceback
import urllib.request, urllib.error, urllib.parse
import bson.json_util

# Our imports
import emission.net.api.visualize as visualize
import emission.net.api.stats as stats
import emission.net.api.usercache as usercache
import emission.net.api.timeline as timeline
import emission.net.api.metrics as metrics
import emission.net.api.pipeline as pipeline

import emission.net.auth.auth as enaa
import emission.net.ext_service.habitica.proxy as habitproxy
from emission.core.wrapper.client import Client
from emission.core.wrapper.user import User
from emission.core.get_database import get_uuid_db, get_mode_db
import emission.core.wrapper.motionactivity as ecwm
import emission.storage.timeseries.timequery as estt
import emission.storage.timeseries.tcquery as esttc
import emission.storage.timeseries.aggregate_timeseries as estag
import emission.storage.timeseries.cache_series as esdc
import emission.core.timer as ect
import emission.core.get_database as edb

try:
    config_file = open('conf/net/api/webserver.conf')
except:
    logging.debug("webserver not configured, falling back to sample, default configuration")
    config_file = open('conf/net/api/webserver.conf.sample')

OPENPATH_URL="https://www.nrel.gov/transportation/openpath.html"

config_data = json.load(config_file)
config_file.close()
static_path = config_data["paths"]["static_path"]
python_path = config_data["paths"]["python_path"]
server_host = config_data["server"]["host"]
server_port = config_data["server"]["port"]
socket_timeout = config_data["server"]["timeout"]
log_base_dir = config_data["paths"]["log_base_dir"]
auth_method = config_data["server"]["auth"]
aggregate_call_auth = config_data["server"]["aggregate_call_auth"]
# not_found_redirect = "foo" if len(config_data["paths"]) == 5 else "bar"
not_found_redirect = config_data["paths"]["404_redirect"] if "404_redirect" in config_data["paths"] else OPENPATH_URL

BaseRequest.MEMFILE_MAX = 1024 * 1024 * 1024 # Allow the request size to be 1G
# to accomodate large section sizes

print("Finished configuring logging for %s" % logging.getLogger())
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

# Backward compat to handle older clients
# Remove in 2023 after everybody has upgraded
# We used to use the presence or absence of the "user" field
# to determine whether this was an aggregate call or not
# now we expect the client to fill it in
def _fill_aggregate_backward_compat(request):
  if 'aggregate' not in request.json:
    # Aggregate if there is no user
    # no aggregate if there is a user
    request.json["aggregate"] = ('user' not in request.json)

@post("/result/heatmap/pop.route/<time_type>")
def getPopRoute(time_type):
  _fill_aggregate_backward_compat(request)
  user_uuid = get_user_or_aggregate_auth(request)

  if 'from_local_date' in request.json and 'to_local_date' in request.json:
      start_time = request.json['from_local_date']
      end_time = request.json['to_local_date']
  else:
      start_time = request.json['start_time']
      end_time = request.json['end_time']

  modes = request.json['modes']
  region = request.json['sel_region']
  logging.debug("Filtering values for user %s, range %s -> %s, region %s" %
        (user_uuid, start_time, end_time, region))
  time_type_map = {
      'timestamp': visualize.range_mode_heatmap_timestamp,
      'local_date': visualize.range_mode_heatmap_local_date
  }
  viz_fn = time_type_map[time_type]
  retVal = viz_fn(user_uuid, modes, start_time, end_time, region)
  return retVal

@post("/result/heatmap/incidents/<time_type>")
def getStressMap(time_type):
    _fill_aggregate_backward_compat(request)
    user_uuid = get_user_or_aggregate_auth(request)

    # modes = request.json['modes']
    # hardcode modes to None because we currently don't store
    # mode information along with the incidents
    # we need to have some kind of cleaned incident that:
    # has a mode
    # maybe has a count generated from clustering....
    # but then what about times?
    modes = None
    if 'from_local_date' in request.json and 'to_local_date' in request.json:
        start_time = request.json['from_local_date']
        end_time = request.json['to_local_date']
    else:
        start_time = request.json['start_time']
        end_time = request.json['end_time']
    region = request.json['sel_region']
    logging.debug("Filtering values for %s, range %s -> %s, region %s" %
                  (user_uuid, start_time, end_time, region))
    time_type_map = {
        'timestamp': visualize.incident_heatmap_timestamp,
        'local_date': visualize.incident_heatmap_local_date
    }
    viz_fn = time_type_map[time_type]
    retVal = viz_fn(user_uuid, modes, start_time, end_time, region)
    return retVal

@post("/pipeline/get_complete_ts")
def getPipelineState():
    user_uuid = getUUID(request)
    return {"complete_ts": pipeline.get_complete_ts(user_uuid)}

@post("/pipeline/get_range_ts")
def getPipelineState():
    user_uuid = getUUID(request)
    (start_ts, end_ts) = pipeline.get_range(user_uuid)
    return {
        "start_ts": start_ts,
        "end_ts": end_ts
    }

@post("/datastreams/find_entries/<time_type>")
def getTimeseriesEntries(time_type):
    if 'user' not in request.json:
        abort(401, "only a user can read his/her data")

    user_uuid = getUUID(request)

    key_list = request.json['key_list']
    if 'from_local_date' in request.json and 'to_local_date' in request.json:
        start_time = request.json['from_local_date']
        end_time = request.json['to_local_date']
        time_key = request.json.get('key_local_date', 'metadata.write_ts')
        time_query = esttc.TimeComponentQuery(time_key,
                                              start_time,
                                              end_time)
    else:
        start_time = request.json['start_time']
        end_time = request.json['end_time']
        time_key = request.json.get('key_time', 'metadata.write_ts')
        time_query = estt.TimeQuery(time_key,
                                    start_time,
                                    end_time)
    # Note that queries from usercache are limited to 100,000 entries
    # and entries from timeseries are limited to 250,000, so we will
    # return at most 350,000 entries. So this means that we don't need
    # additional filtering, but this should be documented in
    # the API
    data_list = esdc.find_entries(user_uuid, key_list, time_query)
    if 'max_entries' in request.json:
        me = request.json['max_entries']
        if (type(me) != int):
            logging.error("aborting: max entry count is %s, type %s, expected int" % (me, type(me)))
            abort(500, "Invalid max_entries %s" % me)

        if len(data_list) > me:
            if request.json['trunc_method'] == 'first':
                logging.debug("first n entries is %s" % me)
                data_list = data_list[:me]
            if request.json['trunc_method'] == 'last':
                logging.debug("first n entries is %s" % me)
                data_list = data_list[-me:]
            elif request.json["trunc_method"] == "sample":
                sample_rate = len(data_list)//me + 1
                logging.debug("sampling rate is %s" % sample_rate)
                data_list = data_list[::sample_rate]
            else:
                logging.error("aborting: unexpected sampling method %s" % request.json["trunc_method"])
                abort(500, "sampling method not specified while retriving limited data")
        else:
            logging.debug("Found %d entries < %s, no truncation" % (len(data_list), me))
    logging.debug("successfully returning list of size %s" % len(data_list))
    return {'phone_data': data_list}

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

@post('/usercache/putone')
def putIntoOneEntry():
  logging.debug("Called userCache.putone with request %s" % request)
  user_uuid=getUUID(request)
  logging.debug("user_uuid %s" % user_uuid)
  the_entry = request.json['the_entry']
  logging.debug("About to save entry %s" % the_entry)
  # sync_phone_to_server requires a list, so we wrap our one entry in the list
  from_phone = [the_entry]
  usercache.sync_phone_to_server(user_uuid, from_phone)
  return {"putone": True}

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
  try:
      logging.debug("Called createUserProfile")
      userEmail = enaa._getEmail(request, auth_method)
      logging.debug("userEmail = %s" % userEmail)
      user = User.register(userEmail)
      logging.debug("Looked up user = %s" % user)
      logging.debug("Returning result %s" % {'uuid': str(user.uuid)})
      return {'uuid': str(user.uuid)}
  except ValueError as e:
      traceback.print_exc()
      abort(403, str(e.args))

@post('/profile/update')
def updateUserProfile():
  logging.debug("Called updateUserProfile")
  user_uuid = getUUID(request)
  user = User.fromUUID(user_uuid)
  new_fields = request.json['update_doc']
  to_return = user.update(new_fields)
  logging.debug("Successfully updated profile for user %s" % user_uuid)
  return {"update": True}

@post('/profile/get')
def getUserProfile():
  logging.debug("Called getUserProfile")
  user_uuid = getUUID(request)
  user = User.fromUUID(user_uuid)
  return user.getProfile()

@post('/result/metrics/<time_type>')
def summarize_metrics(time_type):
    _fill_aggregate_backward_compat(request)
    user_uuid = get_user_or_aggregate_auth(request)

    start_time = request.json['start_time']
    end_time = request.json['end_time']
    freq_name = request.json['freq']
    old_style = False
    if 'metric' in request.json:
        old_style = True
        metric_list = [request.json['metric']]
    else:
        metric_list = request.json['metric_list']

    logging.debug("metric_list = %s" % metric_list)

    if 'is_return_aggregate' in request.json:
        is_return_aggregate = request.json['is_return_aggregate']
    else:
        old_style = True
        is_return_aggregate = True
    time_type_map = {
        'timestamp': metrics.summarize_by_timestamp,
        'local_date': metrics.summarize_by_local_date
    }
    metric_fn = time_type_map[time_type]
    ret_val = metric_fn(user_uuid,
              start_time, end_time,
              freq_name, metric_list, is_return_aggregate)
    # logging.debug("ret_val = %s" % bson.json_util.dumps(ret_val))
    if old_style:
        logging.debug("old_style metrics found, returning array of entries instead of array of arrays")
        assert(len(metric_list) == 1)
        if 'user_metrics' in ret_val:
            ret_val['user_metrics'] = ret_val['user_metrics'][0]
        ret_val['aggregate_metrics'] = ret_val['aggregate_metrics'][0]
    return ret_val

@post('/join.group/<group_id>')
def habiticaJoinGroup(group_id):
    if 'user' in request.json:
        user_uuid = getUUID(request)
    else:
        user_uuid = None
    inviter_id = request.json['inviter']
    logging.debug("%s about to join party %s after invite from %s" %
                  (user_uuid, group_id, inviter_id))
    try:
        ret_val = habitproxy.setup_party(user_uuid, group_id, inviter_id)
        logging.debug("ret_val = %s after joining group" % bson.json_util.dumps(ret_val))
        return {'result': ret_val}
    except RuntimeError as e:
        logging.info("Aborting call with message %s" % e.message)
        abort(400, e.message)

# Small utilities to make client software easier START

# Redirect to custom URL. $%$%$$ gmail
@get('/redirect/<route>')
def getCustomURL(route):
  print(route)
  print(urllib.parse.urlencode(request.query))
  logging.debug("route = %s, query params = %s" % (route, request.query))
  if route == "join":
    redirected_url = "/#/setup?%s" % (urllib.parse.urlencode(request.query))
  else:
    redirected_url = 'emission://%s?%s' % (route, urllib.parse.urlencode(request.query))
  response.status = 303
  response.set_header('Location', redirected_url)
  # response.set_header('Location', 'mailto://%s@%s' % (route, urllib.urlencode(request.query)))
  logging.debug("Redirecting to URL %s" % redirected_url)
  print("Redirecting to URL %s" % redirected_url)
  return {'redirect': 'success'}

# Small utilities to make client software easier END

@post('/habiticaRegister')
def habiticaRegister():
  logging.debug("habitica registration request %s from user = %s" %
                (request.json, request))
  user_uuid = getUUID(request)
  assert(user_uuid is not None)
  username = request.json['regConfig']['username']
  # TODO: Move this logic into register since there is no point in
  # regenerating the password if we already have the user
  autogen_id = requests.get("http://www.dinopass.com/password/simple").text
  logging.debug("generated id %s through dinopass" % autogen_id)
  autogen_email = "%s@save.world" % autogen_id
  autogen_password = autogen_id
  return habitproxy.habiticaRegister(username, autogen_email,
                              autogen_password, user_uuid)

@post('/habiticaProxy')
def habiticaProxy():
    logging.debug("habitica registration request %s" % (request))
    user_uuid = getUUID(request)
    assert(user_uuid is not None)
    method = request.json['callOpts']['method']
    method_url = request.json['callOpts']['method_url']
    method_args = request.json['callOpts']['method_args']
    return habitproxy.habiticaProxy(user_uuid, method, method_url,
                                    method_args)
# Data source integration END

@error(404)
def error404(error):
    response.status = 301
    response.set_header('Location', "https://www.nrel.gov/transportation/openpath.html")

@app.hook('before_request')
def before_request():
  print("START %s %s %s" % (datetime.now(), request.method, request.path))
  request.params.start_ts = time.time()
  request.params.timer = ect.Timer()
  request.params.timer.__enter__()
  logging.debug("START %s %s" % (request.method, request.path))

@app.hook('after_request')
def after_request():
  msTimeNow = time.time()
  request.params.timer.__exit__()
  duration = msTimeNow - request.params.start_ts
  new_duration = request.params.timer.elapsed
  if round(old_div((duration - new_duration), new_duration) > 100) > 0:
    logging.error("old style duration %s != timer based duration %s" % (duration, new_duration))
    stats.store_server_api_error(request.params.user_uuid, "MISMATCH_%s_%s" %
                                 (request.method, request.path), msTimeNow, duration - new_duration)

  print("END %s %s %s %s %s " % (datetime.now(), request.method, request.path, request.params.user_uuid, duration))
  logging.debug("END %s %s %s %s " % (request.method, request.path, request.params.user_uuid, duration))
  # Keep track of the time and duration for each call
  stats.store_server_api_time(request.params.user_uuid, "%s_%s" % (request.method, request.path),
        msTimeNow, duration)
  stats.store_server_api_time(request.params.user_uuid, "%s_%s_cputime" % (request.method, request.path),
        msTimeNow, new_duration)

# Auth helpers BEGIN
# This should only be used by createUserProfile since we may not have a UUID
# yet. All others should use the UUID.

def _get_uuid_bool_wrapper(request):
  try:
    getUUID(request)
    return True
  except:
    return False

def get_user_or_aggregate_auth(request):
  # If this is not an aggregate call, returns the uuid
  # If this is an aggregate call, returns None if the call is valid, otherwise aborts
  # we only support aggregates on a subset of calls, so we don't need the
  # `inHeader` parameter to `getUUID`
  aggregate_call_map = {
    "no_auth": lambda r: None,
    "user_only": lambda r: None if _get_uuid_bool_wrapper(request) else abort(403, "aggregations only available to users"),
    "never": lambda r: abort(404, "Aggregate calls not supported")
  }
  if request.json["aggregate"] == False:
    logging.debug("User specific call, returning UUID")
    return getUUID(request)
  else:
    logging.debug(f"Aggregate call, checking {aggregate_call_auth} policy")
    return aggregate_call_map[aggregate_call_auth](request)

def getUUID(request, inHeader=False):
    try:
        retUUID = enaa.getUUID(request, auth_method, inHeader)
        logging.debug("retUUID = %s" % retUUID)
        if retUUID is None:
           raise HTTPError(403, "token is valid, but no account found for user")
        return retUUID
    except ValueError as e:
        traceback.print_exc()
        abort(403, e)

# Auth helpers END

if __name__ == '__main__':
    try:
        webserver_log_config = json.load(open("conf/log/webserver.conf", "r"))
    except:
        webserver_log_config = json.load(open("conf/log/webserver.conf.sample", "r"))

    print(f"Using auth method {auth_method}")

    logging.config.dictConfig(webserver_log_config)
    logging.debug("This should go to the log file")

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
      key_file = open('conf/net/keys.json')
      key_data = json.load(key_file)
      host_cert = key_data["host_certificate"]
      chain_cert = key_data["chain_certificate"]
      private_key = key_data["private_key"]

      run(host=server_host, port=server_port, server='cheroot', debug=True,
          certfile=host_cert, chainfile=chain_cert, keyfile=private_key)
    else:
      # Non SSL option for testing on localhost
      print("Running with HTTPS turned OFF - use a reverse proxy on production")
      run(host=server_host, port=server_port, server='cheroot', debug=True)

    # run(host="0.0.0.0", port=server_port, server='cherrypy', debug=True)
