import uuid
import requests
import json
import os
import logging

def get_conf_file():
    f = open("conf/net/int_service/giles_conf.json", "r")
    conf = json.loads(f.read())
    f.close()
    return conf

def get_giles_base_url():
    conf = get_conf_file()
    base = conf['giles_base_url']
    return base

def get_giles_api_key():
    conf = get_conf_file()
    api_key = conf['giles_api_key']
    return api_key


class StatArchiver:
    GILES_BASE_URL = get_giles_base_url()
    GILES_API_KEY = get_giles_api_key()
    #QUERY_URL = "http://localhost:8079/api/query"
    #ARCHIVER_URL = "http://localhost:8079/add/apikey"

    def __init__(self, path):
        self.path = path
        self.query_url = os.path.join(self.GILES_BASE_URL, 'api', 'query')
        self.archiver_url = os.path.join(self.GILES_BASE_URL, 'add', self.GILES_API_KEY)

    # NOTE: When we insert an entry to the Archiver, we associate that entry with a stream UUID, which is
    # a function of the stat and user_uuid. 
    # No mapping needs to be maintained since this UUID can be reconstructed as long as the above data is found.
    def insert(self, entry):
        assert type(entry) == dict
        stat = entry['stat']
        user_uuid = str(entry['user'])

        # TODO: Support more precise timestamps
        # Giles has some problem unmarshalling floats
        #client_ts = int(entry['ts'])
        client_ts = int(entry['ts'])
        reading = entry['reading']

        #UUID is a function of things which are stored already, so it seems unimportant to maintain the mapping.
        stream_uuid = str(uuid.uuid5(uuid.NAMESPACE_DNS, stat + ',' + user_uuid))

        smapMsg = {
            self.path: {
                "Metadata": {
                    "SourceName": stat,
                },
                "Properties": {
                     "Timezone": "America/Los_Angeles",
                     "ReadingType": "double",
                     "UnitofTime": "s",
                     "StreamType": "numeric",
                     "UnitofMeasure": "Ambiguous"
                },
                "Readings": [
                    [
                        client_ts,
                        reading
                    ]
                ],
                "uuid": stream_uuid
            }
        }

        for key in entry:
            if key != "reading" and key != 'ts':
                smapMsg[self.path]["Metadata"][key] = entry[key]
        try:
            json.dumps(smapMsg)
        except Exception as e:
            logging.debug("Error storing entry for user %s, stat %s at timestamp %s, with reading %f: entry is not JSON serializable" % (user_uuid, stat, client_ts, reading))
            logging.debug("Exception: " + str(e))
            return None

        # @TODO: Do some error-checking on the response to make sure it actually
        # really did work
        response = requests.post(self.archiver_url, data=json.dumps(smapMsg))
        success = response.content == ''
        return success

    def remove(self):
        queryMsg = 'delete where Path="' + self.path + '"'
        response = requests.post(self.query_url, data=queryMsg)
        return response

    # Tags contain metadata associated with an entry
    # Return all tags for all streams stored in archiver's current path
    def query_tags(self):
        queryMsg = 'select * where Path="' + self.path + '"'
        #print(queryMsg)
        #print(self.query_url)

        response = requests.post(self.query_url, data=queryMsg) 
        try:
            return json.loads(response.content)
        except Exception as e:
            return response.content

    # Return all readings for all streams stored in archiver's current path
    def query_readings(self):
        queryMsg = 'select data before now where Path="' + self.path + '"'
        response = requests.post(self.query_url, data=queryMsg) 
        try:
            return json.loads(response.content)
        except Exception as e:
            return response.content

