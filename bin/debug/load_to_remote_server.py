import logging

import json
import bson.json_util as bju
import argparse

import common
import os

import gzip
import requests
import numpy as np

import emission.simulation.fake_user as esfu
import emission.simulation.client as escg


args = None

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='''
            loads data into a remote debug server through the POST API.
            the remote server *must* have "skip" authentication configured.
            this script does not support token retrieval for google auth or
            open ID connect. `load_timeline_for_day_and_user` is the equivalent
            script for a local server and loads the data directly into the database.
            ''')

    parser.add_argument("-d", "--debug", type=int,
        help="set log level to DEBUG")

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("-c", "--create_spec",
        help="create fake data points by communicating with an OTP server (currently unsupported since we are not running an OTP server)")
    group.add_argument("-l", "--load_file",
        help="load the data from the specified file")

    parser.add_argument("email",
        help="email/label to load the data for")

    parser.add_argument("remote_server",
        help="the remote server to load the data to")

    args = parser.parse_args()
    assert args.create_spec is None, "Creating fake data is currently unsupported"

    client_config = {
        "emission_server_base_url": args.remote_server,
        "register_user_endpoint": "/profile/create",
        "user_cache_endpoint": "/usercache/put"
    }

    user_config = {
            "email" : args.email,
    
            "locations" :
            [
               {
                    'label': 'home',
                    'coordinate': [37.77264255,-122.399714854263]
                },

                {
                    'label': 'work',
                    'coordinate': [37.42870635,-122.140926605802]
                },
                {
                    'label': 'family',
                    'coordinate': [37.87119, -122.27388]
                }
            ],
            "transition_probabilities":
            [
                np.array([ 0.46100375,  0.23107729,  0.30791896]),
                np.array([ 0.46100375,  0.23107729,  0.30791896]),
                np.array([ 0.46100375,  0.23107729,  0.30791896])
            ],
    
            "modes" : 
            {
                "CAR" : [['home', 'family']],
                "TRANSIT" : [['home', 'work'], ['work', 'home']]  
            },

            "default_mode": "CAR",
            "initial_state" : "home",
            "radius" : ".1"
    }

    client = escg.EmissionFakeDataGenerator(client_config)

    # Register the user
    fake_user = client.create_fake_user(user_config)
    entries = json.load(open(args.load_file), object_hook = bju.object_hook)
    fake_user.add_measurements(entries)

    print("About to push %d entries to server %s" %
        (len(fake_user._measurements_cache), client_config["emission_server_base_url"]))

    fake_user.sync_data_to_server()

    print("After push, retained %d entries" % (len(fake_user._measurements_cache)))
