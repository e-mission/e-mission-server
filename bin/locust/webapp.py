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

class PhoneAppUser(HttpUser):
    host = "http://nginxrp/api"
    iteration = 0
    uuid_db_list = list(edb.get_uuid_db().find())

    def wait_time(self):
        self.iteration = self.iteration + 1
        if self.iteration % 100 == 0:
            print(f"Iteration {self.iteration}, wait time = {self.iteration / 100}")
        return self.iteration / 100    
    
    def on_start(self):
       self.opcode = generate_random_string(12)
       self.client.post("/profile/create", json={"user": self.opcode})
       self.src_uuid = secrets.choice(self.uuid_db_list)["uuid"]
       print(f"Connecting existing user {self.src_uuid=} to opcode {self.opcode=}")

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
       self.client.post("/datastreams/find_entries/timestamp",
                        json={"user": self.opcode,
                              "key_list": ["background/location"],
                              "start_time": 0,
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