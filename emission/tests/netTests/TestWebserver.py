
# Standard imports

from builtins import *
import unittest
import json
import sys
import os
import uuid
import logging
import time

# Our imports
import emission.core.deployment_config as ecdc
import emission.tests.common as etc
import emission.net.api.cfc_webapp as enacw
import importlib
from types import SimpleNamespace

class TestWebserver(unittest.TestCase):
    def setUp(self):
        self.originalWebserverEnvVars = {}
        self.testModifiedEnvVars = {
            'WEBSERVER_NOT_FOUND_REDIRECT' : "http://somewhere.else"
        }

        self.orginalDBEnvVars = dict(os.environ)

        for env_var_name, env_var_value in self.testModifiedEnvVars.items():
            # Setting webserver environment variables with test values
            os.environ[env_var_name] = env_var_value

        logging.debug("Finished setting up test webserver environment variables")
        logging.debug("Current original values are = %s" % self.originalWebserverEnvVars)
        logging.debug("Current modified values are = %s" % self.testModifiedEnvVars)

    def tearDown(self):
        logging.debug("Deleting test webserver environment variables")
        etc.restoreOriginalEnvVars(self.originalWebserverEnvVars,
            self.testModifiedEnvVars)
        logging.debug("Finished restoring original webserver environment variables")
        logging.debug("Restored original values are = %s" % self.originalWebserverEnvVars)

    def test404Redirect(self):
        from emission.net.api.bottle import response
        importlib.reload(enacw)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_header("Location"), None)

        enacw.error404("")
        self.assertEqual(response.status_code, 301)
        self.assertEqual(response.get_header("Location"), "http://somewhere.else")

    from unittest import mock
    @mock.patch.dict(os.environ, {"STUDY_CONFIG":"nrel-commute"}, clear=True)
    def test_ResolveAuthWithEnvVar(self):
        importlib.reload(ecdc)
        self.assertEqual(enacw.resolve_auth("dynamic"),"skip")

    @mock.patch.dict(os.environ, {"STUDY_CONFIG":"denver-casr"}, clear=True)
    def test_ResolveAuthWithEnvVar(self):
        importlib.reload(ecdc)
        self.assertEqual(enacw.resolve_auth("dynamic"),"skip")

    @mock.patch.dict(os.environ, {"STUDY_CONFIG":"stage-program"}, clear=True)
    def test_ResolveAuthWithEnvVar(self):
        importlib.reload(ecdc)
        self.assertEqual(enacw.resolve_auth("dynamic"),"token_list")

    def testResolveAuthNoEnvVar(self):
        importlib.reload(ecdc)
        self.assertEqual(enacw.resolve_auth("skip"),"skip")
        self.assertEqual(enacw.resolve_auth("token_list"),"token_list")
        self.assertEqual(enacw.resolve_auth("dynamic"),"token_list")
        self.assertNotEqual(enacw.resolve_auth("dynamic"),"skip")

    def test_bikeshare_checkout_aborts_on_checkout_value_error(self):
        test_uuid = uuid.uuid4()
        req = SimpleNamespace(json={"vehicle_id": "bike-1", "hold_amount_cents": 250})

        with self.assertRaises(RuntimeError):
            with self.mock.patch.object(enacw, "request", req), \
                 self.mock.patch.object(enacw, "getUUID", return_value=test_uuid), \
                 self.mock.patch.object(enacw.vehicle_library, "checkout_vehicle", side_effect=ValueError(404, "Vehicle bike-1 not found")), \
                 self.mock.patch.object(enacw, "abort", side_effect=RuntimeError("abort called")) as mock_abort:
                enacw.bikeshare_checkout()

        mock_abort.assert_called_once_with(404, "Vehicle bike-1 not found")

    def test_bikeshare_checkout_aborts_on_hold_amount_missing(self):
        test_uuid = uuid.uuid4()
        req = SimpleNamespace(json={"vehicle_id": "bike-1"})

        with self.assertRaises(RuntimeError):
            with self.mock.patch.object(enacw, "request", req), \
                 self.mock.patch.object(enacw, "getUUID", return_value=test_uuid), \
                 self.mock.patch.object(enacw, "abort", side_effect=RuntimeError("abort called")) as mock_abort:
                enacw.bikeshare_checkout()

        mock_abort.assert_called_once_with(400, "hold_amount_cents is required")

    def test_bikeshare_return_calls_checkin_with_uuid_and_dock(self):
        test_uuid = uuid.uuid4()
        req = SimpleNamespace(json={"dock_id": "dock-42"})
        expected_result = {"result": "checked_in", "vehicle_id": "bike-1", "dock_id": "dock-42"}

        with self.mock.patch.object(enacw, "request", req), \
             self.mock.patch.object(enacw, "getUUID", return_value=test_uuid), \
             self.mock.patch.object(enacw.vehicle_library, "check_in_vehicle", return_value=expected_result) as mock_checkin:
            result = enacw.bikeshare_return()

        mock_checkin.assert_called_once_with(test_uuid, "dock-42")
        self.assertEqual(result, expected_result)

    def test_bikeshare_return_aborts_on_missing_dock_id(self):
        test_uuid = uuid.uuid4()
        req = SimpleNamespace(json={})

        with self.assertRaises(RuntimeError):
            with self.mock.patch.object(enacw, "request", req), \
                 self.mock.patch.object(enacw, "getUUID", return_value=test_uuid), \
                 self.mock.patch.object(enacw, "abort", side_effect=RuntimeError("abort called")) as mock_abort:
                enacw.bikeshare_return()

        mock_abort.assert_called_once_with(400, "dock_id is required")

    def test_bikeshare_return_aborts_on_checkin_value_error(self):
        test_uuid = uuid.uuid4()
        req = SimpleNamespace(json={"dock_id": "dock-42"})

        with self.assertRaises(RuntimeError):
            with self.mock.patch.object(enacw, "request", req), \
                 self.mock.patch.object(enacw, "getUUID", return_value=test_uuid), \
                 self.mock.patch.object(enacw.vehicle_library, "check_in_vehicle", side_effect=ValueError(403, "No vehicle is currently checked out by this user")), \
                 self.mock.patch.object(enacw, "abort", side_effect=RuntimeError("abort called")) as mock_abort:
                enacw.bikeshare_return()

        mock_abort.assert_called_once_with(403, "No vehicle is currently checked out by this user")

    def test_bikeshare_rental_history_calls_library_with_uuid(self):
        test_uuid = uuid.uuid4()
        req = SimpleNamespace(json={})
        expected_history = [{"data": {"vehicle_id": "bike-1", "rental_status": "completed"}}]

        with self.mock.patch.object(enacw, "request", req), \
             self.mock.patch.object(enacw, "getUUID", return_value=test_uuid), \
             self.mock.patch.object(enacw.vehicle_library, "get_rental_history", return_value=expected_history) as mock_history:
            result = enacw.bikeshare_rental_history()

        mock_history.assert_called_once_with(test_uuid)
        self.assertEqual(result, expected_history)


if __name__ == "__main__":
    etc.configLogging()
    unittest.main()
