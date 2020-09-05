from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
# Standard imports
from future import standard_library
standard_library.install_aliases()
from builtins import *
import unittest
import json
import sys
import os
import uuid
import logging
import time

# Our imports
import emission.tests.common as etc

# It is really hard to test push code, because the tools to emulate pushes are
# kind of sucky. For example, the iOS emulator does not support pushes. And
# even if we use physical devices, are we going to use the same physical
# device? How do we know that the token is valid?
# Therefore, the tests here are primarily about the query and local mapping
# functionality.
# We can add other tests that mock this functionality later
# firebase now maps all tokens, both android and iOS, so we only pass in iOS tokens
# but let's pretend that some of them fail anyway

def generate_fake_result(successful_tokens, failed_tokens):
    fake_result = {
        "results": [{"status": "OK",
                     "registration_token": token.replace("device", "registration"),
                    "apns_token": token} for token in successful_tokens]
    }
    fake_result["results"] = fake_result["results"] + [{"status": "no_matching_token",
                    "apns_token": token} for token in failed_tokens]
    return fake_result


class TestPush(unittest.TestCase):
    def setUp(self):
        import shutil
        self.push_conf_path = "conf/net/ext_service/push.json"
        shutil.copyfile("%s.sample" % self.push_conf_path,
                        self.push_conf_path)
        with open(self.push_conf_path, "w") as fd:
            fd.write(json.dumps({
                "provider": "firebase",
                "server_auth_token": "firebase_api_key",
                "ios_token_format": "apns"
            }))
        logging.debug("Finished setting up %s" % self.push_conf_path)
        with open(self.push_conf_path) as fd:
            logging.debug("Current values are %s" % json.load(fd))

    def tearDown(self):
        os.remove(self.push_conf_path)

    def testGetInterface(self):
        import emission.net.ext_service.push.notify_interface as pni
        import emission.net.ext_service.push.notify_interface_impl.firebase as pnif

        self.assertEqual(pni.NotifyInterfaceFactory.getNotifyInterface("firebase").__class__,
                         pnif.FirebasePush)


    def testGetDefaultInterface(self):
        import emission.net.ext_service.push.notify_interface as pni
        import emission.net.ext_service.push.notify_interface_impl.firebase as pnif

        self.assertEqual(pni.NotifyInterfaceFactory.getDefaultNotifyInterface().__class__,
                         pnif.FirebasePush)

    def testMappingQueries(self):
        import emission.net.ext_service.push.notify_queries as pnq
        import emission.core.wrapper.user as ecwu
        import emission.core.get_database as edb

        self.test_email_1 = "test_push_1"
        self.test_email_2 = "test_push_2"
        try:
            user_1 = ecwu.User.register(self.test_email_1)
            user_2 = ecwu.User.register(self.test_email_2)
            user_1.update({"curr_platform": "ios", "curr_sync_interval": 3600,
                "device_token": "ios_device_token"})
            user_2.update({"curr_platform": "android", "curr_sync_interval": 3600,
                "device_token": "android_device_token"})

            uuid_token_list = pnq.get_matching_tokens(pnq.get_user_query([user_1.uuid, user_2.uuid]))
            self.assertEqual(uuid_token_list, {"ios": ["ios_device_token"],
                "android": ["android_device_token"]})
            
            platform_token_list = pnq.get_matching_tokens(pnq.get_platform_query("ios"))
            self.assertEqual(platform_token_list, {"ios": ["ios_device_token"],
                "android": []})
        finally:
            ecwu.User.unregister(self.test_email_1)
            ecwu.User.unregister(self.test_email_2)

# Currently firebase only!
    def testFcmMapping(self):
        import emission.net.ext_service.push.notify_interface_impl.firebase as pnif
        import emission.core.get_database as edb

        self.test_token_list_ios = ["device_token_ios_%s" % i for i in range(10)]
        self.test_token_list_android = ["device_token_android_%s" % i for i in range(10)]
        self.test_token_map = {"ios": self.test_token_list_ios,
            "android": self.test_token_list_android}
        logging.debug("test token map = %s" % self.test_token_map)

        try:
            fcm_instance = pnif.get_interface({"server_auth_token": "firebase_api_key", "ios_token_format": "apns"})
            (mapped_token_map, unmapped_token_list) = fcm_instance.map_existing_fcm_tokens(self.test_token_map)
            # At this point, there is nothing in the database, so no iOS tokens will be mapped
            self.assertEqual(len(mapped_token_map["ios"]), 0)
            # android tokens should not be mapped, so they will be returned as-is
            self.assertEqual(len(mapped_token_map["android"]), 10)

            # and all iOS tokens will be returned as needing a mapping
            self.assertEqual(len(unmapped_token_list), 10)

            # Now, pretend to get back some mappings
            to_succeed_ios_tokens = self.test_token_list_ios[::2]
            to_fail_ios_tokens = self.test_token_list_ios[1::2]
            fake_result = generate_fake_result(to_succeed_ios_tokens, to_fail_ios_tokens)
            newly_mapped_token_list = fcm_instance.process_fcm_token_result(fake_result)
            logging.debug("to_succeed_ios_tokens = %s, to_fail_ios_tokens = %s, newly_mapped_token_list = %s" % (to_succeed_ios_tokens, to_fail_ios_tokens, newly_mapped_token_list))
            self.assertEqual(len(newly_mapped_token_list), len(to_succeed_ios_tokens))

            # Now try to map again - this time, all the successful ios entries should be mapped, and
            # the android ones not
            (mapped_token_map, unmapped_list) = fcm_instance.map_existing_fcm_tokens(self.test_token_map)
            self.assertEqual(len(mapped_token_map["ios"]), len(to_succeed_ios_tokens))
            self.assertEqual(len(mapped_token_map["android"]), 10)
            self.assertEqual(len(unmapped_list), len(to_fail_ios_tokens))

            to_succeed_ios_tokens_call_2 = to_fail_ios_tokens[::2]
            to_fail_ios_tokens_call_2 = to_fail_ios_tokens[1::2]
            fake_result = generate_fake_result(to_succeed_ios_tokens_call_2, to_fail_ios_tokens_call_2)
            # Fake the mapped result again
            newly_mapped_token_list = fcm_instance.process_fcm_token_result(fake_result)
            logging.debug("to_succeed_ios_tokens_call_2 = %s, to_fail_ios_tokens_call_2 = %s, newly_mapped_token_list = %s" % (to_succeed_ios_tokens_call_2, to_fail_ios_tokens_call_2, newly_mapped_token_list))
            self.assertEqual(len(newly_mapped_token_list), len(to_succeed_ios_tokens_call_2))

            # Now try to map again - again, all the ios entries that succeeded
            # in the past two calls should be mapped, and the failed ones not
            (mapped_token_map, unmapped_list) = fcm_instance.map_existing_fcm_tokens(self.test_token_map)
            self.assertEqual(len(mapped_token_map["ios"]),
                len(to_succeed_ios_tokens) + len(to_succeed_ios_tokens_call_2))
            self.assertEqual(len(mapped_token_map["android"]), 10)
            self.assertEqual(len(unmapped_list), len(to_fail_ios_tokens_call_2))

        finally:
            # Delete everything from the database
            edb.get_push_token_mapping_db().delete_many({})

    def testFcmNoMapping(self):
        import emission.net.ext_service.push.notify_interface_impl.firebase as pnif
        import emission.core.get_database as edb

        self.test_token_list_ios = ["device_token_ios_%s" % i for i in range(10)]
        self.test_token_list_android = ["device_token_android_%s" % i for i in range(10)]
        self.test_token_map = {"ios": self.test_token_list_ios,
            "android": self.test_token_list_android}
        logging.debug("test token map = %s" % self.test_token_map)

        fcm_instance = pnif.get_interface({"server_auth_token": "firebase_api_key", "ios_token_format": "fcm"})
        (mapped_token_map, unmapped_token_list) = fcm_instance.map_existing_fcm_tokens(self.test_token_map)
        # These are assumed to be FCM tokens directly, so no mapping required
        self.assertEqual(len(mapped_token_map["ios"]), 10)
        # android tokens should not be mapped, so they will be returned as-is
        self.assertEqual(len(mapped_token_map["android"]), 10)

        # no tokens will be returned as needing a mapping
        self.assertEqual(len(unmapped_token_list), 0)
        
        # and there will be no entries in the token mapping database
        self.assertEqual(edb.get_push_token_mapping_db().count_documents({}), 0)
    
if __name__ == '__main__':
    import emission.tests.common as etc

    etc.configLogging()
    unittest.main()
