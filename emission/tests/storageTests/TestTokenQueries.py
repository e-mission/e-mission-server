import unittest
import logging
import uuid
import json

#changed all script runs from os() to subprocess.run() for consistency
#TODO clean up commented out os() lines
# import os
import subprocess 

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
        for u in edb.get_uuid_db().find({}):
            if len(u["user_email"]) == 1:
                edb.get_uuid_db().delete_one(u)

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

    def test_run_script_file(self):
        # os.system("python3 bin/auth/insert_tokens.py --file emission/tests/storageTests/token_test_files/tokens.txt")
        subprocess.run(["python3", "bin/auth/insert_tokens.py", "--file", "emission/tests/storageTests/token_test_files/tokens.txt"])
        self.assertEqual(esdt.get_all_tokens(), ['a','b','c','d'])

    def test_run_script_file_auth(self):
        # os.system("python3 bin/auth/insert_tokens.py --file emission/tests/storageTests/token_test_files/tokens.txt")
        subprocess.run(["python3", "bin/auth/insert_tokens.py", "--file", "emission/tests/storageTests/token_test_files/tokens.txt"])
        tlm = enat.TokenListMethod()
        self.assertEqual(tlm.verifyUserToken('b'), 'b')

    def test_run_script_file_auth_fail(self):
        # os.system("python3 bin/auth/insert_tokens.py --file emission/tests/storageTests/token_test_files/tokens.txt")
        subprocess.run(["python3", "bin/auth/insert_tokens.py", "--file", "emission/tests/storageTests/token_test_files/tokens.txt"])
        with self.assertRaises(ValueError):
            tlm = enat.TokenListMethod()
            tlm.verifyUserToken('z')

    def test_run_script_single(self):
        # os.system("python3 bin/auth/insert_tokens.py --single a")
        subprocess.run(["python3", "bin/auth/insert_tokens.py", "--single", "a"])
        # os.system("python3 bin/auth/insert_tokens.py --single b")
        subprocess.run(["python3", "bin/auth/insert_tokens.py", "--single", "b"])
        self.assertEqual(esdt.get_all_tokens(), ['a','b'])

    def test_run_script_single_auth(self):
        # os.system("python3 bin/auth/insert_tokens.py --single a")
        subprocess.run(["python3", "bin/auth/insert_tokens.py", "--single", "a"])
        # os.system("python3 bin/auth/insert_tokens.py --single b")
        subprocess.run(["python3", "bin/auth/insert_tokens.py", "--single", "b"])
        tlm = enat.TokenListMethod()
        self.assertEqual(tlm.verifyUserToken('b'), 'b')

    def test_run_script_single_auth_fail(self):
        # os.system("python3 bin/auth/insert_tokens.py --single a")
        subprocess.run(["python3", "bin/auth/insert_tokens.py", "--single", "a"])
        # os.system("python3 bin/auth/insert_tokens.py --single b")
        subprocess.run(["python3", "bin/auth/insert_tokens.py", "--single", "b"])
        with self.assertRaises(ValueError):
            tlm = enat.TokenListMethod()
            tlm.verifyUserToken('z')

    def test_run_script_uuid(self):
        edb.get_uuid_db().insert_one({"user_email":"a"})
        edb.get_uuid_db().insert_one({"user_email":"b"})
        edb.get_uuid_db().insert_one({"user_email":"c"})
        edb.get_uuid_db().insert_one({"user_email":"d"})
        # os.system("python3 bin/auth/insert_tokens.py --uuid")
        subprocess.run(["python3", "bin/auth/insert_tokens.py", "--uuid"])
        self.assertEqual(esdt.get_all_tokens(), ['a','b','c','d'])

    def test_run_script_uuid_auth(self):
        edb.get_uuid_db().insert_one({"user_email":"a"})
        edb.get_uuid_db().insert_one({"user_email":"b"})
        edb.get_uuid_db().insert_one({"user_email":"c"})
        edb.get_uuid_db().insert_one({"user_email":"d"})
        # os.system("python3 bin/auth/insert_tokens.py --uuid")
        subprocess.run(["python3", "bin/auth/insert_tokens.py", "--uuid"])
        tlm = enat.TokenListMethod()
        self.assertEqual(tlm.verifyUserToken('b'), 'b')

    def test_run_script_uuid_auth_fail(self):
        edb.get_uuid_db().insert_one({"user_email":"a"})
        edb.get_uuid_db().insert_one({"user_email":"b"})
        edb.get_uuid_db().insert_one({"user_email":"c"})
        edb.get_uuid_db().insert_one({"user_email":"d"})
        # os.system("python3 bin/auth/insert_tokens.py --uuid")
        subprocess.run(["python3", "bin/auth/insert_tokens.py", "--uuid"])
        with self.assertRaises(ValueError):
            tlm = enat.TokenListMethod()
            tlm.verifyUserToken('z')

    def test_run_script_show(self):
        esdt.insert({'token':'x'})
        esdt.insert({'token':'y'})
        esdt.insert({'token':'z'})
        sp = subprocess.run(["python3", "bin/auth/insert_tokens.py", "--show"], capture_output=True)
        # The first message is displayed when we run tests locally
        # The second is displayed when we run in the docker CI, since the `DB_HOST` is set to `db`
        self.assertIn(sp.stdout,
            [b'storage not configured, falling back to sample, default configuration\nURL not formatted, defaulting to "Stage_database"\nConnecting to database URL localhost\nx\ny\nz\n',
            b'URL not formatted, defaulting to "Stage_database"\nConnecting to database URL db\nx\ny\nz\n'
        ])

    def test_run_script_empty(self):
        sp = subprocess.run(["python3", "bin/auth/insert_tokens.py"], capture_output=True)
        # The first message is displayed when we run tests locally
        # The second is displayed when we run in the docker CI, since the `DB_HOST` is set to `db`
        self.assertIn(sp.stdout,
            [b'storage not configured, falling back to sample, default configuration\nURL not formatted, defaulting to "Stage_database"\nConnecting to database URL localhost\nPlease provide the script with an argument. Use the "--help" option for more details\n',
            b'URL not formatted, defaulting to "Stage_database"\nConnecting to database URL db\nPlease provide the script with an argument. Use the "--help" option for more details\n'
        ])

    #test that no two options can be used together
    def test_run_script_mutex(self):
        #code will be anded with returncode of each subprocess
        #an unsuccessful run is indicated by some number other than 0
        #a single successful run will force code to be stuck at 0
        code = 1
        sp = subprocess.run(["python3", "bin/auth/insert_tokens.py", "--show", "--file", "emission/tests/storageTests/token_test_files/tokens.txt"])
        code = code and sp.returncode
        sp = subprocess.run(["python3", "bin/auth/insert_tokens.py", "--show", "--single", "z"])
        code = code and sp.returncode
        sp = subprocess.run(["python3", "bin/auth/insert_tokens.py", "--show", "uuid"])
        code = code and sp.returncode
        sp = subprocess.run(["python3", "bin/auth/insert_tokens.py", "--file", "emission/tests/storageTests/token_test_files/tokens.txt", "single", "z"])
        code = code and sp.returncode
        sp = subprocess.run(["python3", "bin/auth/insert_tokens.py", "--file", "emission/tests/storageTests/token_test_files/tokens.txt", "uuid"])
        code = code and sp.returncode
        sp = subprocess.run(["python3", "bin/auth/insert_tokens.py", "--uuid", "single", "z"])
        code = code and sp.returncode

        #if code is 0, at least one run was successful, which we don't want
        self.assertNotEqual(code, 0)

if __name__ == '__main__':
    import emission.tests.common as etc
    etc.configLogging()
    unittest.main()
