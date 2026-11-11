from builtins import *
import logging
import unittest
import uuid

import emission.core.get_database as edb
import emission.core.wrapper.dock as ecwd
import emission.storage.modifiable.abstract_state_storage as esas


class TestStateStorage(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s',
            level=logging.DEBUG)

        self.user_id = uuid.uuid4()
        self.state_storage = esas.StateStorage.get_state_storage(self.user_id)

    def tearDown(self):
        edb.get_state_db().delete_many({"user_id": self.user_id})

    def test_state_enum_contains_dock(self):
        self.assertEqual(esas.StateName.DOCK.value, "DOCK")

    def test_state_enum_to_wrapper_mapping(self):
        state_mapping = esas.StateStorage._getStateName2Wrapper()

        self.assertEqual(state_mapping[esas.StateName.DOCK], "dock")
        self.assertEqual(
            esas.StateStorage.get_state_wrapper("DOCK"),
            "dock",
        )

    def test_upsert_and_get_current_state_for_dock(self):
        dock_state = ecwd.Dock({
            "dock_id": "dock_123",
            "dock_name": "Dock A",
        })
        self.state_storage.upsert_state(esas.StateName.DOCK, dock_state)

        current_state = self.state_storage.get_current_state(esas.StateName.DOCK)
        self.assertIsNotNone(current_state)
        self.assertTrue(isinstance(current_state, ecwd.Dock))
        self.assertEqual(current_state["dock_id"], "dock_123")
        self.assertEqual(current_state["dock_name"], "Dock A")

    def test_upsert_state_replaces_existing_state_for_same_state_name(self):
        self.state_storage.upsert_state(
            esas.StateName.DOCK,
            ecwd.Dock({
                "dock_id": "dock_old",
                "dock_name": "Old Dock",
            }),
        )
        self.state_storage.upsert_state(
            esas.StateName.DOCK,
            ecwd.Dock({
                "dock_id": "dock_new",
                "dock_name": "New Dock",
            }),
        )

        current_state = self.state_storage.get_current_state(esas.StateName.DOCK)
        self.assertEqual(current_state["dock_id"], "dock_new")
        self.assertEqual(current_state["dock_name"], "New Dock")

        dock_count = edb.get_state_db().count_documents(
            {"user_id": self.user_id, "metadata.key": "state/dock"}
        )
        self.assertEqual(dock_count, 1)

    def test_upsert_state_rejects_dict(self):
        with self.assertRaises(TypeError):
            self.state_storage.upsert_state(
                esas.StateName.DOCK,
                {"placeholder": "value"},
            )


if __name__ == '__main__':
    import emission.tests.common as etc

    etc.configLogging()
    unittest.main()
