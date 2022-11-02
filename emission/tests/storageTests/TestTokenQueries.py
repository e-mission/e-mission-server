import unittest
import logging
import uuid
import json

import os

import emission.core.get_database as edb

import emission.storage.decorations.token_queries as esdt
import emission.net.auth.token_list as enat


class TestTokenQueries(unittest.TestCase):

    def tearDown(self):
    #All tests insert tokens of length one. Delete them once the test is done.
    #Shouldn't affect "real" tokens with greater length that might be in the DB
        for t in edb.get_token_db().find({}):
            if len(t["token"]) == 1:
                edb.get_token_db().delete_one(t)

    def test_single_insert(self):
        esdt.insert({'token':'z'})
        self.assertEqual(esdt.get_all_tokens(), ['z'])

    def test_mult_insert(self):
        esdt.insert({'token':'z'})
        esdt.insert({'token':'y'})
        esdt.insert({'token':'x'})
        self.assertEqual(esdt.get_all_tokens(), ['z','y','x'])

    def test_insert_many_entries(self):
        tl = [{'token':'1'},{'token':'2'},{'token':'3'}]
        esdt.insert_many_entries(tl)
        self.assertEqual(esdt.get_all_tokens(), ['1','2','3'])

    def test_insert_many_tokens(self):
        tl = ['1','2','3']
        esdt.insert_many_tokens(tl)
        self.assertEqual(esdt.get_all_tokens(), ['1','2','3'])

    def test_get_tokens_from_file(self):
        tl = esdt.get_tokens_from_file("emission/tests/storageTests/token_test_files/tokens.txt")
        self.assertEqual(tl, ['a','b','c','d'])

    def test_auth_manual_insert(self):
        esdt.insert({'token':'z'})
        esdt.insert({'token':'y'})
        esdt.insert({'token':'x'})
        tlm = enat.TokenListMethod()
        self.assertEqual(tlm.verifyUserToken('y'), 'y')

    def test_auth_fail_manual_insert(self):
        esdt.insert({'token':'z'})
        esdt.insert({'token':'y'})
        esdt.insert({'token':'x'})
        with self.assertRaises(ValueError):
            tlm = enat.TokenListMethod()
            tlm.verifyUserToken('a')

    def test_auth_file_insert(self):
        tl = esdt.get_tokens_from_file("emission/tests/storageTests/token_test_files/tokens.txt")
        esdt.insert_many_tokens(tl)
        tlm = enat.TokenListMethod()
        self.assertEqual(tlm.verifyUserToken('a'), 'a')

    def test_auth_fail_file_insert(self):
        tl = esdt.get_tokens_from_file("emission/tests/storageTests/token_test_files/tokens.txt")
        esdt.insert_many_tokens(tl)
        tlm = enat.TokenListMethod()
        with self.assertRaises(ValueError):
            tlm = enat.TokenListMethod()
            tlm.verifyUserToken('z')

    def test_run_script(self):
        os.system("python3 bin/auth/insert_tokens.py emission/tests/storageTests/token_test_files/tokens.txt")
        self.assertEqual(esdt.get_all_tokens(), ['a','b','c','d'])

    def test_run_script_auth(self):
        os.system("python3 bin/auth/insert_tokens.py emission/tests/storageTests/token_test_files/tokens.txt")
        tlm = enat.TokenListMethod()
        self.assertEqual(tlm.verifyUserToken('b'), 'b')

    def test_run_script_auth_fail(self):
        os.system("python3 bin/auth/insert_tokens.py emission/tests/storageTests/token_test_files/tokens.txt")
        with self.assertRaises(ValueError):
            tlm = enat.TokenListMethod()
            tlm.verifyUserToken('z')

if __name__ == '__main__':
    import emission.tests.common as etc
    etc.configLogging()
    unittest.main()