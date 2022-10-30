import unittest
import logging
import uuid
import json

import os

import emission.core.get_database as edb

import emission.storage.decorations.token_queries as esdt
import emission.net.auth.token_list as enat

import bin.auth.insert_tokens as bai

class TestTokenQueries(unittest.TestCase):
    def setUp(self):
        self.testUserId = uuid.uuid3(uuid.NAMESPACE_URL, "mailto:test@test.me")
        edb.get_token_db().delete_many({})

    def test_single_insert(self):
        esdt.insert({'user_id':self.testUserId,'token':'z'})
        self.assertEqual(esdt.get_all_tokens(), ['z'])

    def test_mult_insert(self):
        esdt.insert({'user_id':self.testUserId,'token':'z'})
        esdt.insert({'user_id':self.testUserId,'token':'y'})
        esdt.insert({'user_id':self.testUserId,'token':'x'})
        self.assertEqual(esdt.get_all_tokens(), ['z','y','x'])

    def test_auth_manual_insert(self):
        esdt.insert({'user_id':self.testUserId,'token':'z'})
        esdt.insert({'user_id':self.testUserId,'token':'y'})
        esdt.insert({'user_id':self.testUserId,'token':'x'})
        tlm = enat.TokenListMethod()
        self.assertEqual(tlm.verifyUserToken('y'), 'y')

    def test_auth_fail_manual_insert(self):
        esdt.insert({'user_id':self.testUserId,'token':'z'})
        esdt.insert({'user_id':self.testUserId,'token':'y'})
        esdt.insert({'user_id':self.testUserId,'token':'x'})
        with self.assertRaises(ValueError):
            tlm = enat.TokenListMethod()
            tlm.verifyUserToken('a')

    def test_script_insert(self):
        tl = bai.get_tokens_from_file("emission/tests/storageTests/token_test_files/tokens.txt")
        bai.load_tokens_into_db(tl, self.testUserId)
        self.assertEqual(esdt.get_all_tokens(), ['a','b','c','d'])

    def test_auth_script_insert(self):
        tl = bai.get_tokens_from_file("emission/tests/storageTests/token_test_files/tokens.txt")
        bai.load_tokens_into_db(tl, self.testUserId)
        tlm = enat.TokenListMethod()
        self.assertEqual(tlm.verifyUserToken('c'), 'c')

    def test_auth_fail_script_insert(self):
        tl = bai.get_tokens_from_file("emission/tests/storageTests/token_test_files/tokens.txt")
        bai.load_tokens_into_db(tl, self.testUserId)
        with self.assertRaises(ValueError):
            tlm = enat.TokenListMethod()
            tlm.verifyUserToken('z')

    def test_run_script(self):
        os.system("python3 bin/auth/insert_tokens.py emission/tests/storageTests/token_test_files/tokens.txt {}".format(self.testUserId))
        self.assertEqual(esdt.get_all_tokens(), ['a','b','c','d'])

    def test_run_script_auth(self):
        os.system("python3 bin/auth/insert_tokens.py emission/tests/storageTests/token_test_files/tokens.txt {}".format(self.testUserId))
        tlm = enat.TokenListMethod()
        self.assertEqual(tlm.verifyUserToken('b'), 'b')

    def test_run_script_auth_fail(self):
        os.system("python3 bin/auth/insert_tokens.py emission/tests/storageTests/token_test_files/tokens.txt {}".format(self.testUserId))
        with self.assertRaises(ValueError):
            tlm = enat.TokenListMethod()
            tlm.verifyUserToken('z')

if __name__ == '__main__':
    import emission.tests.common as etc
    etc.configLogging()
    unittest.main()