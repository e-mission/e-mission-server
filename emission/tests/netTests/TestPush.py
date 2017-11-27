# Standard imports
import unittest
import json
import sys
import os
import uuid
import logging
import time

# Our imports
import emission.tests.common as etc

import emission.net.ext_service.push.notify_interface as pni
import emission.net.ext_service.push.notify_usage as pnu

# It is really hard to test push code, because the tools to emulate pushes are
# kind of sucky. For example, the iOS emulator does not support pushes. And
# even if we use physical devices, are we going to use the same physical
# device? How do we know that the token is valid?
# Therefore, the tests here are primarily about the query and local mapping
# functionality.
# We can add other tests that mock this functionality later

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
        open(self.push_conf_path, "w").write(json.dumps({
            "provider": "firebase",
            "server_auth_token": "firebase_api_key"
        }))

    def tearDown(self):
        # os.remove(self.push_conf_path)
        pass

    def testGetInterface(self):
        import emission.net.ext_service.push.notify_interface_impl.firebase as pnif

        self.assertEqual(pni.NotifyInterfaceFactory.getNotifyInterface("firebase").__class__,
                         pnif.FirebasePush)

        import emission.net.ext_service.push.notify_interface_impl.ionic as pnii

        self.assertEqual(pni.NotifyInterfaceFactory.getNotifyInterface("ionic").__class__,
                         pnii.IonicPush)

    def testGetDefaultInterface(self):
        import emission.net.ext_service.push.notify_interface_impl.firebase as pnif

        self.assertEqual(pni.NotifyInterfaceFactory.getDefaultNotifyInterface().__class__,
                         pnif.FirebasePush)

        self.assertEqual(pnu.__get_default_interface__().__class__,
                         pnif.FirebasePush)

    def testMappingQueries(self):
        import emission.core.wrapper.user as ecwu

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
            self.assertEqual(token_list, ["ios_device_token", "android_device_token"])
            
            platform_token_list = pnq.get_matching_tokens(pnq.get_platform_query("ios"))
            self.assertEqual(token_list, ["ios_device_token"])
        finally:
            ecwu.User.unregister(self.test_email_1)
            ecwu.User.unregister(self.test_email_2)

# Currently firebase only!
    def testFcmMapping(self):
        import emission.net.ext_service.push.notify_interface_impl.firebase as pnif
        import emission.core.get_database as edb

        self.test_token_list = ["device_token_%s" % i for i in range(10)]
        logging.debug("test token list = %s" % self.test_token_list)

        try:
            fcm_instance = pnif.get_interface({"server_auth_token": "firebase_api_key"})
            (mapped_list, unmapped_list) = fcm_instance.map_existing_fcm_tokens(self.test_token_list)
            # At this point, there is nothing in the database, so nothing will be mapped
            self.assertEqual(len(mapped_list), 0)
            self.assertEqual(len(unmapped_list), 10)

            # Now, pretend to get back some mappings
            pretend_ios_tokens = self.test_token_list[:5:2]
            pretend_android_tokens = self.test_token_list[6::2] + self.test_token_list[1::2]
            fake_result = generate_fake_result(pretend_ios_tokens, pretend_android_tokens)
            newly_mapped_token_list = fcm_instance.process_fcm_token_result(self.test_token_list, fake_result)
            logging.debug("pretend_ios_tokens = %s, pretend_android_tokens = %s, newly_mapped_token_list = %s" % (pretend_ios_tokens, pretend_android_tokens, newly_mapped_token_list))
            self.assertEqual(len(newly_mapped_token_list), len(self.test_token_list))

            # Now try to map again - this time, all the ios entries should be mapped, and
            # the android ones not
            (mapped_list, unmapped_list) = fcm_instance.map_existing_fcm_tokens(self.test_token_list)
            self.assertEqual(len(mapped_list), len(pretend_ios_tokens))
            self.assertEqual(len(unmapped_list), len(pretend_android_tokens))

            pretend_ios_tokens = self.test_token_list[::2]
            pretend_android_tokens = self.test_token_list[1::2]
            fake_result = generate_fake_result(pretend_ios_tokens, pretend_android_tokens)
            # Fake the mapped result again
            newly_mapped_token_list = fcm_instance.process_fcm_token_result(self.test_token_list, fake_result)
            logging.debug("pretend_ios_tokens = %s, pretend_android_tokens = %s, newly_mapped_token_list = %s" % (pretend_ios_tokens, pretend_android_tokens, newly_mapped_token_list))
            self.assertEqual(len(newly_mapped_token_list), len(self.test_token_list))

            # Now try to map again - again, all the ios entries should be mapped, and
            # the android ones not
            (mapped_list, unmapped_list) = fcm_instance.map_existing_fcm_tokens(self.test_token_list)

        finally:
            # Delete everything from the database
            edb.get_push_token_mapping_db().remove()
    
if __name__ == '__main__':
    import emission.tests.common as etc

    etc.configLogging()
    unittest.main()
