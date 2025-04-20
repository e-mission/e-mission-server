import string
import secrets
from uuid import UUID
import os
import arrow
from locust import HttpUser, task, constant

import emission.core.get_database as edb
import emission.core.wrapper.user as ecwu
import emission.storage.json_wrappers as esj

def generate_random_string(length=12):
    """Generates a random string of the specified length."""
    characters = string.ascii_letters + string.digits
    return ''.join(secrets.choice(characters) for _ in range(length))

def random_src_random_created_dest(self):
    self.opcode = generate_random_string(12)
    self.client.post("/profile/create", json={"user": self.opcode})
    self.src_uuid = secrets.choice(uuid_db_list)["uuid"]
    print(f"Connecting existing user {self.src_uuid=} to opcode {self.opcode=}")
    self.key_to_read = "background/filtered_location"

def random_src_single_existing_dest(self):
    self.opcode = "some_dest_opcode_REPLACEME"
    self.src_uuid = secrets.choice(uuid_db_list)["uuid"]
    self.key_to_read = "background/filtered_location"
    print(f"Connecting existing source user {self.src_uuid=} to existing dest opcode {self.opcode=}")

def same_src_dest_random(self):
    # we can't really copy data from a different user in the same program
    # because it may overlap with the existing users' data and mess everything up
    # so we just copy the data from the same user if that task is enabled
    chosen_user = secrets.choice(uuid_db_list)
    self.opcode = chosen_user["user_email"]
    self.src_uuid = chosen_user["uuid"]
    self.key_to_read = "analysis/composite_trip"
    print(f"Simulating existing user with {self.src_uuid=} and {self.opcode=}")

def backoff_wait_time(self):
    self.iteration = self.iteration + 1
    if self.iteration % 100 == 0:
        print(f"Iteration {self.iteration}, wait time = {self.iteration / 100}")
    return self.iteration / 100

uuid_db_list = list(edb.get_uuid_db().find())

class PhoneAppUser(HttpUser):
    # host = "https://openpath-stage.nrel.gov/api"
    host = "http://nginxrp/api"
    iteration = 0

    def wait_time(self):
        return backoff_wait_time(self)
        # return 5
    
    def on_start(self):
        # use this to copy data from an existing dump to a new dummy local
        # program, this will focus on writes
        # random_src_random_created_dest(self)
        # use this with staging, remember to fill in the single opcode
        # random_src_single_existing_dest(self)
        # use this with when the existing dump has already been loaded in the
        # database under load; this will focus on reads
        same_src_dest_random(self)


    @task(3)
    def profile_update(self):
       self.client.post("/profile/update",
                        json={"user": self.opcode,
                              "update_doc": {"platform": "locust", "app_version": "1.9.3"}})

    @task(2)
    def usercache_get(self):
       self.client.post("/usercache/get", json={"user": self.opcode})

    @task(2)
    def find_entries_timestamp(self):
       start_ts = 0 if self.key_to_read == "background/location" else \
            arrow.utcnow().shift(days=-7).timestamp()
       self.client.post("/datastreams/find_entries/timestamp",
                        json={"user": self.opcode,
                              "key_list": [self.key_to_read],
                              "start_time": start_ts,
                              "end_time": arrow.utcnow().timestamp()})

    @task
    def usercache_put(self):
       entries = list(edb.get_timeseries_db().find({"user_id": self.src_uuid}).sort("metadata.key", 1).limit(10000))
       for e in entries:
           del e["_id"]
           del e["user_id"]
           if "type" not in e["metadata"]:
               e["metadata"]["type"] = "missing"
       wrapped_json = {"user": self.opcode, "phone_to_server": entries}
       self.client.post("/usercache/put", data=esj.wrapped_dumps(wrapped_json),
                        headers={"Content-Type": "application/json"})

    @task
    def metrics(self):
       DEFAULT_METRIC_LIST = {
           'footprint': ['mode_confirm'],
           'distance': ['mode_confirm'],
           'duration': ['mode_confirm'],
           'count': ['mode_confirm'],
       }
       wrapped_json = {"user": self.opcode,
          "metric_list": DEFAULT_METRIC_LIST,
          "start_time": arrow.utcnow().shift(days=-7).isoformat(),
          "end_time": arrow.utcnow().isoformat(),
          "is_return_aggregate": True if self.iteration % 2 == 0 else False,
          "freq": "D"}
       self.client.post("/result/metrics/yyyy_mm_dd", data=esj.wrapped_dumps(wrapped_json),
                         headers={"Content-Type": "application/json"})