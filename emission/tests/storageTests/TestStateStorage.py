from builtins import *
import logging
import unittest
import uuid

import emission.core.get_database as edb
import emission.core.wrapper.payment as ecwp
import emission.core.wrapper.rental as ecwr
import emission.storage.modifiable.abstract_state_storage as esas


class TestStateStorage(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s',
            level=logging.DEBUG)

        self.user_id = uuid.uuid4()
        self.state_storage = esas.StateStorage.get_state_storage(self.user_id)

    def tearDown(self):
        edb.get_state_db().delete_many({"user_id": self.user_id})

    def test_state_enum_contains_rental_and_payment(self):
        self.assertEqual(esas.StateName.RENTAL.value, "RENTAL")
        self.assertEqual(esas.StateName.PAYMENT.value, "PAYMENT")

    def test_state_enum_to_wrapper_mapping(self):
        state_mapping = esas.StateStorage._getStateName2Wrapper()

        self.assertEqual(state_mapping[esas.StateName.RENTAL], "rental")
        self.assertEqual(state_mapping[esas.StateName.PAYMENT], "payment")
        self.assertEqual(
            esas.StateStorage.get_state_wrapper(esas.StateName.RENTAL),
            "rental",
        )
        self.assertEqual(
            esas.StateStorage.get_state_wrapper("PAYMENT"),
            "payment",
        )

    def test_upsert_and_get_current_state_for_vehicle(self):
        vehicle_state = ecwr.Rental({
            "vehicle_id": "veh-001",
            "vehicle_name": "new vehicle added",
            "payment_hold_info": {"id": "pi_001"},
            "rental_start_ts": 100,
            "rental_end_ts": 200,
            "rental_status": "active",
        })
        self.state_storage.upsert_state(esas.StateName.RENTAL, vehicle_state)

        current_state = self.state_storage.get_current_state(esas.StateName.RENTAL)
        self.assertIsNotNone(current_state)
        self.assertTrue(isinstance(current_state, ecwr.Rental))
        self.assertEqual(current_state["vehicle_name"], "new vehicle added")

    def test_upsert_and_get_current_state_for_payment(self):
        payment_state = ecwp.Payment({
            "customer_id": "cust_123",
            "payment_method_id": "pm_123",
            "pending_setup_session_id": "seti_123",
        })
        self.state_storage.upsert_state(esas.StateName.PAYMENT, payment_state)

        current_state = self.state_storage.get_current_state(esas.StateName.PAYMENT)
        self.assertIsNotNone(current_state)
        self.assertTrue(isinstance(current_state, ecwp.Payment))
        self.assertEqual(current_state["customer_id"], "cust_123")
        self.assertEqual(current_state["payment_method_id"], "pm_123")
        self.assertEqual(current_state["pending_setup_session_id"], "seti_123")

    def test_upsert_state_replaces_existing_state_for_same_state_name(self):
        self.state_storage.upsert_state(
            esas.StateName.RENTAL,
            ecwr.Rental({
                "vehicle_id": "veh-old",
                "vehicle_name": "old",
                "payment_hold_info": {"id": "pi_old"},
                "rental_start_ts": 100,
                "rental_end_ts": 150,
                "rental_status": "active",
            }),
        )
        self.state_storage.upsert_state(
            esas.StateName.RENTAL,
            ecwr.Rental({
                "vehicle_id": "veh-new",
                "vehicle_name": "new",
                "payment_hold_info": {"id": "pi_new"},
                "rental_start_ts": 200,
                "rental_end_ts": 250,
                "rental_status": "active",
            }),
        )

        current_state = self.state_storage.get_current_state(esas.StateName.RENTAL)
        self.assertEqual(current_state["vehicle_name"], "new")

        rental_count = edb.get_state_db().count_documents(
            {"user_id": self.user_id, "metadata.key": "state/rental"}
        )
        self.assertEqual(rental_count, 1)

    def test_upsert_state_rejects_dict(self):
        with self.assertRaises(TypeError):
            self.state_storage.upsert_state(
                esas.StateName.RENTAL,
                {"placeholder": "value"},
            )


if __name__ == '__main__':
    import emission.tests.common as etc

    etc.configLogging()
    unittest.main()
