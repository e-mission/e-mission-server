from builtins import *
import logging
import unittest
import uuid

import emission.core.get_database as edb
import emission.core.wrapper.payment as ecwp
import emission.core.wrapper.vehicle as ecwvs
import emission.storage.modifiable.abstract_state_storage as esas


class TestStateStorage(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s',
            level=logging.DEBUG)

        self.user_id = uuid.uuid4()
        self.state_storage = esas.StateStorage.get_state_storage(self.user_id)

    def tearDown(self):
        edb.get_state_db().delete_many({"user_id": self.user_id})

    def test_state_enum_contains_vehicle_and_payment(self):
        self.assertEqual(esas.StateName.VEHICLE.value, "VEHICLE")
        self.assertEqual(esas.StateName.PAYMENT.value, "PAYMENT")

    def test_state_enum_to_wrapper_mapping(self):
        state_mapping = esas.StateStorage._getStateName2Wrapper()

        self.assertEqual(state_mapping[esas.StateName.VEHICLE], "vehicle")
        self.assertEqual(state_mapping[esas.StateName.PAYMENT], "payment")
        self.assertEqual(
            esas.StateStorage.get_state_wrapper(esas.StateName.VEHICLE),
            "vehicle",
        )
        self.assertEqual(
            esas.StateStorage.get_state_wrapper("PAYMENT"),
            "payment",
        )

    def test_upsert_and_get_current_state_for_vehicle(self):
        vehicle_state = ecwvs.Vehicle({"placeholder": "new vehicle added"})
        self.state_storage.upsert_state(esas.StateName.VEHICLE, vehicle_state)

        current_state = self.state_storage.get_current_state(esas.StateName.VEHICLE)
        self.assertIsNotNone(current_state)
        self.assertTrue(isinstance(current_state, ecwvs.Vehicle))
        self.assertEqual(current_state["placeholder"], "new vehicle added")

    def test_upsert_and_get_current_state_for_payment(self):
        payment_state = ecwp.Payment({"placeholder": "settled"})
        self.state_storage.upsert_state(esas.StateName.PAYMENT, payment_state)

        current_state = self.state_storage.get_current_state(esas.StateName.PAYMENT)
        self.assertIsNotNone(current_state)
        self.assertTrue(isinstance(current_state, ecwp.Payment))
        self.assertEqual(current_state["placeholder"], "settled")

    def test_upsert_state_replaces_existing_state_for_same_state_name(self):
        self.state_storage.upsert_state(
            esas.StateName.VEHICLE,
            ecwvs.Vehicle({"placeholder": "old"}),
        )
        self.state_storage.upsert_state(
            esas.StateName.VEHICLE,
            ecwvs.Vehicle({"placeholder": "new"}),
        )

        current_state = self.state_storage.get_current_state(esas.StateName.VEHICLE)
        self.assertEqual(current_state["placeholder"], "new")

        vehicle_count = edb.get_state_db().count_documents(
            {"user_id": self.user_id, "metadata.key": "state/vehicle"}
        )
        self.assertEqual(vehicle_count, 1)

    def test_upsert_state_rejects_dict(self):
        with self.assertRaises(TypeError):
            self.state_storage.upsert_state(
                esas.StateName.VEHICLE,
                {"placeholder": "value"},
            )


if __name__ == '__main__':
    import emission.tests.common as etc

    etc.configLogging()
    unittest.main()
