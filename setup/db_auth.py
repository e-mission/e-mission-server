# This script sets up authentication with the following setup
# - one ADMIN_USER
# - one RW_USER
# - one RO_USER with createIndex privileges

import sys
import pymongo
import argparse
import traceback

# Variables to change

DB_HOST="localhost"
ADMIN_USERNAME="test-admin"
ADMIN_PASSWORD="test-admin-pw"
RW_USERNAME="test-rw"
RW_PASSWORD="test-rw-pw"
RO_USERNAME="test-ro"
RO_PASSWORD="test-ro-pw"

class SetupDBAuth(object):
    def __init__(self):
    # At this point, there is no authentication
        pass

    def getURL(self, username, password):
        return ("mongodb://%s:%s@%s/admin?authMechanism=SCRAM-SHA-1" % 
            (username, password, DB_HOST))

    # First set up the admin user
    # We will open a new connection instead of using the configured URL because
    # that may change later to include a username and password
    def setupAdminUser(self):
        self.admin_default = pymongo.MongoClient(DB_HOST).admin
        create_result = self.admin_default.command(
          {
            "createUser": ADMIN_USERNAME,
            "pwd": ADMIN_PASSWORD,
            "roles": [ { "role": "userAdminAnyDatabase", "db": "admin" } ]
          }
        )
        self.admin_auth = pymongo.MongoClient(self.getURL(ADMIN_USERNAME, ADMIN_PASSWORD)).admin
        print("Created admin user, result = %s" % create_result)
        print("At current state, list of users = %s" % self.admin_auth.command({"usersInfo": 1}))

    def teardownAdminUser(self):
        try:
            self.admin_default = pymongo.MongoClient(DB_HOST).admin
            drop_result = self.admin_default.command(
              {
                "dropUser": ADMIN_USERNAME
              }
            )
            self.admin_auth = None
            print("Dropped admin user, result = %s" % drop_result)
            print("At current state, list of users = %s" % self.admin_default.command({"usersInfo": 1}))
        except Exception as e:
            traceback.print_exc(limit=5, file=sys.stdout)
            print("Error while dropping admin user, skipping")

    def setupRWUser(self):
        create_result = self.admin_auth.command(
          {
            "createUser": RW_USERNAME,
            "pwd": RW_PASSWORD,
            "roles": [ { "role": "readWrite", "db": "Stage_database" } ]
          }
        )
        print("Created RW user, result = %s" % create_result)
        print("At current state, list of users = %s" % self.admin_auth.command({"usersInfo": 1}))

    def teardownRWUser(self):
        try:
            drop_result = self.admin_auth.command(
              {
                "dropUser": RW_USERNAME,
              }
            )
            print("Dropped RW user, result = %s" % drop_result)
            print("At current state, list of users = %s" % self.admin_auth.command({"usersInfo": 1}))
        except Exception as e:
            traceback.print_exc(limit=5, file=sys.stdout)
            print("Error while dropping RW user, skipping")

    def setupROUser(self):
        self.stagedb_auth = pymongo.MongoClient(
            self.getURL(ADMIN_USERNAME, ADMIN_PASSWORD)).Stage_database
        create_role_result = self.stagedb_auth.command(
          {
            "createRole": "createIndex",
             "privileges": [
                { "resource": { "db": "Stage_database", "collection": "" },
                                "actions": [ "createIndex"] }
              ],
              "roles": []
          }
        )
        print("Created new role, result = %s" % create_role_result)
        print("At current state, list of roles = %s" % 
            self.stagedb_auth.command({ "rolesInfo": 1, "showBuiltinRoles": False, "showPrivileges": True}))
        create_result = self.admin_auth.command(
          {
            "createUser": RO_USERNAME,
            "pwd": RO_PASSWORD,
            "roles": [ { "role": "read", "db": "Stage_database" },
                       { "role": "createIndex", "db": "Stage_database"} ]
          }
        )
        print("Created RO user, result = %s" % create_result)
        print("At current state, list of users = %s" % self.admin_auth.command({"usersInfo": 1}))

    def teardownROUser(self):
        try:
            self.stagedb_auth = pymongo.MongoClient(
                self.getURL(ADMIN_USERNAME, ADMIN_PASSWORD)).Stage_database
            drop_role_result = self.stagedb_auth.command(
              {
                "dropRole": "createIndex"
              }
            )
            print("Dropped new role, result = %s" % drop_role_result)
            print("At current state, list of roles = %s" % 
                self.stagedb_auth.command({ "rolesInfo": 1, "showBuiltinRoles": False, "showPrivileges": True}))
        except Exception as e:
            traceback.print_exc(limit=5, file=sys.stdout)
            print("Error while dropping role, skipping")

        try:        
            drop_result = self.admin_auth.command(
              {
                "dropUser": RO_USERNAME
              }
            )
            print("Dropped RO user, result = %s" % drop_result)
            print("At current state, list of users = %s" % self.admin_auth.command({"usersInfo": 1}))
        except Exception as e:
            traceback.print_exc(limit=5, file=sys.stdout)
            print("Error while dropping ro user, skipping")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog="db_auth", epilog="Run this script against a database without authentication - e.g. mongod *without* --auth")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("-s", "--setup",
        help="create users and roles in the database", action='store_true')
    group.add_argument("-t", "--teardown",
        help="remove users and roles created by this script from the database.", action='store_true')
    args = parser.parse_args()

    sad = SetupDBAuth()
    if args.setup:
        sad.setupAdminUser()
        sad.setupRWUser()
        sad.setupROUser()
    else:
        assert(args.teardown == True)
        sad.admin_auth = pymongo.MongoClient(sad.getURL(ADMIN_USERNAME, ADMIN_PASSWORD)).admin
        sad.teardownROUser()
        sad.teardownRWUser()
        sad.teardownAdminUser()
