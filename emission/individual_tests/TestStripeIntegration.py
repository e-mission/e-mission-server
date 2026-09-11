from builtins import *
import json
import os
import unittest
import uuid

import stripe

# Ensure stripe_service import does not crash if env var is absent.
os.environ.setdefault("STRIPE_SECRET_KEY", "")

import emission.core.wrapper.payment as ecwp
import emission.core.wrapper.user as ecwu
import emission.net.ext_service.stripe.stripe_service as stripe_service


class TestStripeIntegration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.api_key = os.environ.get("STRIPE_SECRET_KEY", "")
        if not cls.api_key or cls.api_key == "sk_test_dummy":
            raise unittest.SkipTest("Set a real STRIPE_SECRET_KEY to run Stripe integration tests")
        if not cls.api_key.startswith("sk_test"):
            raise unittest.SkipTest("Integration tests only run with Stripe test keys (sk_test*)")

        stripe.api_key = cls.api_key
        stripe.api_base = stripe_service.STRIPE_API_BASE

    def setUp(self):
        self.test_email = f"stripe-integration-{uuid.uuid4()}@example.com"
        self.test_uuid = ecwu.User.register(self.test_email).uuid
        self._created_customer_ids = []

    def tearDown(self):
        for customer_id in self._created_customer_ids:
            try:
                stripe.Customer.delete(customer_id)
            except Exception:
                pass
        ecwu.User.unregister(self.test_email)

    def _setup_payment_method(self):
        customer = stripe.Customer.create(
            description=f"e-mission integration customer {self.test_uuid}",
        )
        customer_json = json.loads(str(customer))
        customer_id = customer_json["id"]
        self._created_customer_ids.append(customer_id)

        payment_method = stripe.PaymentMethod.create(
            type="card",
            card={"token": "tok_visa"},
        )
        payment_method_json = json.loads(str(payment_method))
        payment_method_id = payment_method_json["id"]

        stripe.PaymentMethod.attach(payment_method_id, customer=customer_id)

        payment_db = stripe_service.esas.StateStorage.get_state_storage(self.test_uuid)
        payment_db.delete_state(stripe_service.esas.StateName.PAYMENT)

        payment_state = ecwp.Payment()
        payment_state.payment_setup_status = ecwp.PaymentSetupStatus.SUCCEEDED
        payment_state.stripe_customer_id = customer_id
        payment_state.payment_setup = {
            "payment_method": payment_method_id,
        }
        payment_db.upsert_state(stripe_service.esas.StateName.PAYMENT, payment_state)

        return customer_id, payment_method_id

    def _create_hold_intent(self, amount_cents):
        hold_intent = stripe_service.create_hold_payment_intent(
            self.test_uuid,
            amount_cents,
            metadata={"source": "TestStripeIntegration"},
        )
        self.assertEqual(hold_intent["amount"], amount_cents)
        self.assertEqual(hold_intent["capture_method"], "manual")
        self.assertEqual(hold_intent["status"], "requires_capture")
        return hold_intent

    def test_capture_amount_zero(self):
        self._setup_payment_method()

        hold_intent = self._create_hold_intent(250)
        payment_intent_id = hold_intent["id"]

        result = stripe_service.capture_hold_payment_intent(payment_intent_id, 0)
        self.assertIsNone(result)

        refreshed = json.loads(str(stripe.PaymentIntent.retrieve(payment_intent_id)))
        self.assertEqual(refreshed.get("status"), "canceled")

    def test_capture_amount_partial(self):
        self._setup_payment_method()

        max_amount = 300
        capture_amount = 120
        hold_intent = self._create_hold_intent(max_amount)
        payment_intent_id = hold_intent["id"]

        result = stripe_service.capture_hold_payment_intent(payment_intent_id, capture_amount)
        self.assertEqual(result.get("id"), payment_intent_id)
        self.assertEqual(result.get("amount_received"), capture_amount)

        refreshed = json.loads(str(stripe.PaymentIntent.retrieve(payment_intent_id)))
        self.assertEqual(refreshed.get("status"), "succeeded")

        with self.assertRaisesRegex(
            stripe.error.InvalidRequestError,
            "You cannot cancel this PaymentIntent because it has a status of succeeded",
        ):
            stripe_service.cancel_hold_payment_intent(payment_intent_id)

    def test_capture_amount_max(self):
        self._setup_payment_method()

        max_amount = 250
        hold_intent = self._create_hold_intent(max_amount)
        payment_intent_id = hold_intent["id"]

        result = stripe_service.capture_hold_payment_intent(payment_intent_id, max_amount)
        self.assertEqual(result.get("id"), payment_intent_id)
        self.assertEqual(result.get("amount_received"), max_amount)

        refreshed = json.loads(str(stripe.PaymentIntent.retrieve(payment_intent_id)))
        self.assertEqual(refreshed.get("status"), "succeeded")

        with self.assertRaisesRegex(
            stripe.error.InvalidRequestError,
            "You cannot cancel this PaymentIntent because it has a status of succeeded",
        ):
            stripe_service.cancel_hold_payment_intent(payment_intent_id)

    def test_capture_amount_above_max_fails(self):
        self._setup_payment_method()

        max_amount = 250
        hold_intent = self._create_hold_intent(max_amount)
        payment_intent_id = hold_intent["id"]

        with self.assertRaises(stripe.error.InvalidRequestError) as err_ctx:
            stripe_service.capture_hold_payment_intent(payment_intent_id, max_amount + 1)

        err_str = str(err_ctx.exception)
        self.assertTrue(
            "amount_to_capture" in err_str or "greater than" in err_str,
            msg=f"Unexpected Stripe error for above-max capture: {err_str}",
        )

        refreshed = json.loads(str(stripe.PaymentIntent.retrieve(payment_intent_id)))
        self.assertEqual(refreshed.get("status"), "requires_capture")

        cancelled = stripe_service.cancel_hold_payment_intent(payment_intent_id)
        self.assertEqual(cancelled.get("status"), "canceled")


if __name__ == '__main__':
    import emission.tests.common as etc

    etc.configLogging()
    unittest.main()
