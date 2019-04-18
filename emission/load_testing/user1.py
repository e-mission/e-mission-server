from locust import HttpLocust, TaskSet, task
from emission.load_testing.random_email_gen import id_generator
from emission.simulation.client import EmissionFakeDataGenerator
from bin.purge_database import purgeAllData
from emission.simulation.fake_user import FakeUser
import json

#TODO: Seems like there is no need for DataGenClient now that we are using locust. Need to rewrite some stuff.
#Currently breaking a lot of abstraction barriers.

class UserBehavior(TaskSet):
    def on_start(self):
        """ on_start is called when a Locust start before any task is scheduled """
        self._user_conf_file = open('emission/load_testing/conf/user1.json')
        self._client_conf_file = open('emission/load_testing/conf/datagen_client.json')
        user_config = json.load(self._user_conf_file)
        #Create a random email for this user.
        user_config['email'] = id_generator(20)
        client_config = json.load(self._client_conf_file)
        self._config = client_config

        self.user = self.create_fake_user(user_config)

    def on_stop(self):
        """ on_stop is called when the TaskSet is stopping """
        #Close files
        self._user_conf_file.close()
        self._client_conf_file.close()
        #Purge Database
        purgeAllData()

    @task(1)
    def take_trip(self):
        measurements = self.user.take_trip()
        self.user._flush_cache()

        measurements_no_id = [self._remove_id_field(entry) for entry in measurements]
        data = {
            'phone_to_server': measurements_no_id,
            'user': self.user._email
        }
        url = self._config['user_cache_endpoint']

        r = self.client.post(url, json=data)
        # Check if successful
        if r.ok:
            print("%d entries were successfully synced to the server" % len(measurements_no_id))
        else:
            print(
                'Something went wrong when trying to sync your data. Try again or use save_cache_to_file to save your data.')
            print(r.content)

    @task(2)
    def sync_data_to_server(self):
        pass


    def create_fake_user(self, config):
        #TODO: parse the config object
        uuid = self._register_fake_user(config['email'])
        config['uuid'] = uuid
        config['upload_url'] = self._config['emission_server_base_url'] + self._config['user_cache_endpoint']
        return FakeUser(config)

    def _register_fake_user(self, email):
        data = {'user': email}
        url = self._config['register_user_endpoint']
        r = self.client.post(url, json=data)
        r.raise_for_status()
        uuid = r.json()['uuid']
        #TODO: This is a hack to make all the genereated entries JSON encodeable.
        #Might be a bad Idead to stringify the uuid. For instance,
        # the create_entry function expects uuid of type UUID
        return str(uuid)

    @staticmethod
    def _remove_id_field(entry):
        copy = entry.copy()
        del copy['_id']
        return copy

class WebsiteUser(HttpLocust):
    task_set = UserBehavior
    min_wait = 5000
    max_wait = 9000