import unittest
import logging
import uuid
import json

import emission.core.get_database as edb

import emission.storage.decorations.token_queries as esdt
import emission.net.auth.token_list as tl


class TestPlaceQueries(unittest.TestCase):
    def setUp(self):
        self.testUserId = uuid.uuid3(uuid.NAMESPACE_URL, "mailto:test@test.me")
        edb.get_token_db().delete_many({})

    def test_insert(self):
        esdt.insert({'user_id':self.testUserId,'token':'z'})
        self.assertEqual(esdt.get_all_tokens(), ['z'])

    def test_insert_from_json(self):
        esdt.insert_from_json('emission/tests/storageTests/token_test_files/token_list.json', self.testUserId)
        esdt.get_all_tokens()
        self.assertEqual(esdt.get_all_tokens(), ['a','b','c','d'])

    def test_auth(self):
        tlm = tl.TokenListMethod('emission/tests/storageTests/token_test_files/token_list.json', self.testUserId)
        self.assertEqual(tlm.verifyUserToken('c'), 'c')

if __name__ == '__main__':
    import emission.tests.common as etc
    etc.configLogging()
    unittest.main()