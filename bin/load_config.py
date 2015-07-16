import logging
import json
import sys
import usercache.abstract_usercache as ucauc
from dao.user import User

def load_config(config_json):
    """
    Loads the specified config file into the specified user cache.
    If there is an existing entry with the same key in the cache, it will be overwritten.
    If there is no existing entry with the same key in the cache, an entry will be inserted.
    """
    for entry in config_json:
        userEmail = entry['userEmail']
        userObj = User.fromEmail(userEmail)
        if userObj is None:
            print "user with email %s is not found, ignoring...." % userEmail
        else:
            uc = ucauc.UserCache.getUserCache(userObj.uuid)
            key = entry['key']
            value = entry['value']
            uc.putDocument(key, value)
            print "successfully loaded key %s for user %s" % (key, userEmail)

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print "USAGE: %s config_file \n" % sys.argv[0]
        exit(1)

    config_filename = sys.argv[1]
    print "Loading config data form file %s" % config_filename
    config_json = json.load(open(config_filename))
    load_config(config_json)
