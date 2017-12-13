# Special test used for checking mongodb auth stuff
# Implemented outside tests because it requires mongod to be started with the
# --auth opton to pass
# $ mongod --auth
# And if there are failures, and teardown doesn't work properly, mongodb needs
# to be restarted without auth (e.g. $ mongod) and the users and roles dropped
# >>> stagedb = MongoClient('localhost').Stage_database
# >>> stagedb.command({"dropAllRolesFromDatabase": 1})
# >>> admin = MongoClient('localhost').Stage_database
# >>> admin.command({"dropAllUsersFromDatabase": 1})
# And then restarted with `--auth` to run the tests
# if mongodb is run without `--auth`, the readonly test is guaranteed to fail
#
# Also, since the test loads the configuration URL on import, and modules are
# only imported once per run, the tests have to be run one by one.
# Working around that adds too much complexity for now and this is not an
# automatically run test anyway


from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

from future import standard_library
standard_library.install_aliases()
from builtins import *

import unittest
import datetime as pydt
import logging
import uuid
import json
import pymongo
import importlib
import os

class TestMongodbAuth(unittest.TestCase):
    def setUp(self):
        self.admin_default = pymongo.MongoClient('localhost').admin
        self.test_username = "test-admin"
        self.test_password = "test-admin-password"
        self.uuid = uuid.uuid4()
        self.testUserId = self.uuid
        self.db_conf_file = "conf/storage/db.conf"
        self.createAdmin()

    def tearDown(self):
        self.admin_auth.command({"dropAllUsersFromDatabase": 1})
        try:
            os.remove(self.db_conf_file)
        except FileNotFoundError as e:
            logging.info("File %s not found, nothing to remove" % self.db_conf_file)

    def createAdmin(self):
        self.admin_default.command(
          {
            "createUser": self.test_username,
            "pwd": self.test_password,
            "roles": [ { "role": "userAdminAnyDatabase", "db": "admin" } ]
          }
        )
        self.admin_auth = pymongo.MongoClient(self.getURL(self.test_username, self.test_password)).admin

    def configureDB(self, url):
        config = {
            "timeseries": {
                "url": url
            }
        }
        with open(self.db_conf_file, "w") as fp:
            json.dump(config, fp, indent=4)

    def getURL(self, username, password):
        return "mongodb://%s:%s@localhost/admin?authMechanism=SCRAM-SHA-1" % (username, password)

    def testCreateAdmin(self):
        result = self.admin_auth.command({"usersInfo": self.test_username})
        self.assertEqual(result['ok'], 1.0)
        self.assertEqual(len(result['users']), 1)
        self.assertEqual(result['users'][0]['user'], self.test_username)

    def testReadWriteUser(self):
        try:
            rw_username = "test-rw"
            rw_password = "test-rw-password"
            self.admin_auth.command(
              {
                "createUser": rw_username,
                "pwd": rw_password,
                "roles": [ { "role": "readWrite", "db": "Stage_database" } ]
              }
            )
            result = self.admin_auth.command({"usersInfo": rw_username})
            self.assertEqual(result['ok'], 1.0)
            self.assertEqual(len(result['users']), 1)
            self.assertEqual(result['users'][0]['user'], rw_username)

            self.configureDB(self.getURL(rw_username, rw_password))

            import emission.tests.storageTests.analysis_ts_common as etsa
            import emission.storage.decorations.analysis_timeseries_queries as esda
            import emission.core.wrapper.rawplace as ecwrp
            import emission.storage.timeseries.abstract_timeseries as esta

            ts = esta.TimeSeries.get_time_series(self.uuid)
            etsa.createNewPlaceLike(self, esda.RAW_PLACE_KEY, ecwrp.Rawplace)
     
            inserted_df = ts.get_data_df(esda.RAW_PLACE_KEY)
            self.assertEqual(len(inserted_df), 1)
            self.assertEqual(len(ts.get_data_df(esda.CLEANED_PLACE_KEY)), 0)
        finally:
            import emission.core.get_database as edb

            edb.get_analysis_timeseries_db().delete_many({'user_id': self.testUserId})

    def testReadOnlyUser(self):
        try:
            ro_username = "test-ro"
            ro_password = "test-ro-password"
            self.stagedb_auth = pymongo.MongoClient(self.getURL(self.test_username, self.test_password)).Stage_database
            self.stagedb_auth.command(
              {
                "createRole": "createIndex",
                 "privileges": [
                    { "resource": { "db": "Stage_database", "collection": "" },
                                    "actions": [ "createIndex"] }
                  ],
                  "roles": []
              }
            )
            role_result = self.stagedb_auth.command({ "rolesInfo": 1, "showBuiltinRoles": False, "showPrivileges": True})
            logging.debug("role_result = %s" % role_result)
            self.assertEqual(role_result['ok'], 1.0)
            self.assertEqual(len(role_result['roles']), 1)
            self.assertEqual(role_result['roles'][0]['role'], "createIndex")
            self.assertEqual(role_result['roles'][0]['db'], "Stage_database")
            self.assertEqual(len(role_result['roles'][0]['privileges']), 1)
            self.assertEqual(role_result['roles'][0]['privileges'][0]["actions"], ["createIndex"])

            self.admin_auth.command(
              {
                "createUser": ro_username,
                "pwd": ro_password,
                "roles": [ { "role": "read", "db": "Stage_database" },
                           { "role": "createIndex", "db": "Stage_database"} ]
              }
            )
            result = self.admin_auth.command({"usersInfo": ro_username})
            self.assertEqual(result['ok'], 1.0)
            self.assertEqual(len(result['users']), 1)
            self.assertEqual(result['users'][0]['user'], ro_username)

            self.configureDB(self.getURL(ro_username, ro_password))

            import emission.tests.storageTests.analysis_ts_common as etsa
            import emission.storage.decorations.analysis_timeseries_queries as esda
            import emission.core.wrapper.rawplace as ecwrp
            import emission.storage.timeseries.abstract_timeseries as esta

            ts = esta.TimeSeries.get_time_series(self.uuid)
            with self.assertRaises(pymongo.errors.OperationFailure):
                etsa.createNewPlaceLike(self, esda.RAW_PLACE_KEY, ecwrp.Rawplace)
     
            inserted_df = ts.get_data_df(esda.RAW_PLACE_KEY)
            self.assertEqual(len(inserted_df), 0)
            self.assertEqual(len(ts.get_data_df(esda.CLEANED_PLACE_KEY)), 0)
        finally:
            import emission.core.get_database as edb

            with self.assertRaises(pymongo.errors.OperationFailure):
                edb.get_analysis_timeseries_db().delete_many({'user_id': self.testUserId})
            self.stagedb_auth.command({"dropAllRolesFromDatabase": 1})

if __name__ == '__main__':
    # import emission.tests.common as etc
    # etc.configLogging()
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
