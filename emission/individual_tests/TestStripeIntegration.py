from builtins import *
import json
import os
import unittest
import uuid
import logging

import stripe

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

    def _log_stripe_result(self, label, result):
        try:
            payload = result if isinstance(result, dict) else json.loads(str(result))
        except Exception:
            payload = str(result)
        logging.info(f"{label}: {payload}")

    def _setup_payment_method(self, card_token="tok_visa"):
        customer = stripe.Customer.create(
            description=f"e-mission integration customer {self.test_uuid}",
        )
        customer_json = json.loads(str(customer))
        self._log_stripe_result("stripe customer create", customer_json)
        customer_id = customer_json["id"]
        self._created_customer_ids.append(customer_id)

        payment_method = stripe.PaymentMethod.create(
            type="card",
            card={"token": card_token},
        )
        payment_method_json = json.loads(str(payment_method))
        self._log_stripe_result("stripe payment method create", payment_method_json)
        payment_method_id = payment_method_json["id"]

        attached_payment_method = stripe.PaymentMethod.attach(payment_method_id, customer=customer_id)
        self._log_stripe_result("stripe payment method attach", attached_payment_method)

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
        self._log_stripe_result("stripe hold payment intent", hold_intent)
        self.assertEqual(hold_intent["amount"], amount_cents)
        self.assertEqual(hold_intent["capture_method"], "manual")
        self.assertEqual(hold_intent["status"], "requires_capture")
        return hold_intent

    def test_capture_amount_zero(self):
        self._setup_payment_method()

        hold_intent = self._create_hold_intent(250)
        payment_intent_id = hold_intent["id"]

        result = stripe_service.capture_hold_payment_intent(payment_intent_id, 0)
        self._log_stripe_result("stripe capture result zero", result)
        self.assertIsNone(result)

        refreshed = json.loads(str(stripe.PaymentIntent.retrieve(payment_intent_id)))
        self._log_stripe_result("stripe payment intent after zero capture", refreshed)
        self.assertEqual(refreshed.get("status"), "canceled")

    def test_capture_amount_partial(self):
        self._setup_payment_method()

        max_amount = 300
        capture_amount = 120
        hold_intent = self._create_hold_intent(max_amount)
        payment_intent_id = hold_intent["id"]

        result = stripe_service.capture_hold_payment_intent(payment_intent_id, capture_amount)
        self._log_stripe_result("stripe partial capture result", result)
        self.assertEqual(result.get("id"), payment_intent_id)
        self.assertEqual(result.get("amount_received"), capture_amount)

        refreshed = json.loads(str(stripe.PaymentIntent.retrieve(payment_intent_id)))
        self._log_stripe_result("stripe payment intent after partial capture", refreshed)
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
        self._log_stripe_result("stripe max capture result", result)
        self.assertEqual(result.get("id"), payment_intent_id)
        self.assertEqual(result.get("amount_received"), max_amount)

        refreshed = json.loads(str(stripe.PaymentIntent.retrieve(payment_intent_id)))
        self._log_stripe_result("stripe payment intent after max capture", refreshed)
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

        with self.assertRaises(ValueError) as err_ctx:
            stripe_service.capture_hold_payment_intent(payment_intent_id, max_amount + 1)

        err_str = str(err_ctx.exception)
        logging.debug(f"stripe above-max capture error: {err_str}")
        self.assertTrue(
            "amount_to_capture" in err_str or "greater than" in err_str,
            msg=f"Unexpected Stripe error for above-max capture: {err_str}",
        )

        refreshed = json.loads(str(stripe.PaymentIntent.retrieve(payment_intent_id)))
        self._log_stripe_result("stripe payment intent after failed above-max capture", refreshed)
        self.assertEqual(refreshed.get("status"), "requires_capture")

        cancelled = stripe_service.cancel_hold_payment_intent(payment_intent_id)
        self._log_stripe_result("stripe cancel after failed above-max capture", cancelled)
        self.assertEqual(cancelled.get("status"), "canceled")

    def test_two_captures_against_single_hold(self):
        self._setup_payment_method()

        hold_intent = self._create_hold_intent(500)
        payment_intent_id = hold_intent["id"]

        first_capture = stripe_service.capture_hold_payment_intent(payment_intent_id, 150)
        self._log_stripe_result("stripe first capture result", first_capture)
        self.assertEqual(first_capture.get("id"), payment_intent_id)
        self.assertEqual(first_capture.get("amount_received"), 150)

        after_first_capture = json.loads(str(stripe.PaymentIntent.retrieve(payment_intent_id)))
        self._log_stripe_result("stripe payment intent after first capture", after_first_capture)
        self.assertEqual(after_first_capture.get("status"), "succeeded")

        with self.assertRaises(ValueError) as second_capture_err:
            stripe_service.capture_hold_payment_intent(payment_intent_id, 100)

        logging.debug(f"stripe second capture error: {second_capture_err.exception}")
        err_str = str(second_capture_err.exception)
        self.assertTrue(
            "succeeded" in err_str or "unexpected state" in err_str or "cannot be captured" in err_str or "remainder of the authorized amount has been released" in err_str,
            msg=f"Unexpected Stripe error for second capture: {err_str}",
        )

    def test_bad_test_cards_raise_during_hold_or_capture(self):
        bad_card_cases = [
            ("tok_chargeCustomerFail", ["declined", "card_declined"]),
            ("tok_visa_chargeCustomerFailLostCard", ["lost_card", "declined", "card_declined"]),
        ]

        for card_token, expected_fragments in bad_card_cases:
            with self.subTest(card_token=card_token):
                self._setup_payment_method(card_token=card_token)

                def attempt_hold_and_capture():
                    hold_intent = self._create_hold_intent(250)
                    stripe_service.capture_hold_payment_intent(hold_intent["id"], 125)

                with self.assertRaises(Exception) as err_ctx:
                    attempt_hold_and_capture()

                err_str = str(err_ctx.exception)
                logging.debug(f"stripe error for {card_token}: {type(err_ctx.exception)} -> {err_str}")
                self.assertTrue(
                    any(fragment in err_str for fragment in expected_fragments),
                    msg=f"Unexpected Stripe error for {card_token}: {err_str}",
                )


if __name__ == '__main__':
    import emission.tests.common as etc

    etc.configLogging()
    unittest.main()
