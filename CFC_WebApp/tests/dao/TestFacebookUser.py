import unittest
import sys
import os
import json

from facebook import GraphAPI, get_app_access_token
from dao.socialmediauser import FacebookUser
from dao.user import User

sys.path.append("%s" % os.getcwd())

from dao.client import Client
from tests import common

import logging

logging.basicConfig(level=logging.DEBUG)


class TestFacebookUser(unittest.TestCase):

    def setUp(self):
        fb_config = open('clients/socialmedia/settings.json')
        fb_data = json.load(fb_config)['client_settings']
        self.app_id = fb_data['fb_app_id']
        self.secret_key = fb_data['fb_app_secret']

    def testFromAccessToken(self):
        app_access_token = get_app_access_token(self.app_id, self.secret_key)
        g = GraphAPI(app_access_token)
        users = g.request('%s/accounts/test-users' % self.app_id)
        user = users['data'][0]
        access_token = user['access_token']
        g2 = GraphAPI(access_token)
        email = g2.get_object('me')['email']
        client = Client("testclient")
        client.update(createKey=False)
        common.makeValid(client)
        (resultPre, resultReg) = client.preRegister("this_is_the_super_secret_id", email)
        self.assertEqual(resultPre, 1)
        self.assertEqual(resultReg, 0)
        user = User.register(email)
        fbuser = FacebookUser.from_access_token(access_token)
        self.assertEquals(fbuser.uuid, user.uuid)




if __name__ == '__main__':
    unittest.main()
