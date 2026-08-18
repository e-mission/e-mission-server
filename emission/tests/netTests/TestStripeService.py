from builtins import *
import unittest
from unittest.mock import patch

import emission.core.wrapper.user as ecwu
import emission.core.wrapper.payment as ecwp
import emission.net.ext_service.stripe.stripe_service as stripe_service


class TestStripeService(unittest.TestCase):
    def setUp(self):
        self.test_email = f"stripe-{self._testMethodName}@example.com"
        self.test_uuid = ecwu.User.register(self.test_email).uuid

    def tearDown(self):
        ecwu.User.unregister(self.test_email)

    def test_create_setup_checkout_session_success(self):
        fake_session = {
            'id': 'cs_setup_123',
            'url': 'https://checkout.stripe.com/c/pay/cs_setup_123',
            'status': 'open',
        }

        with patch.object(stripe_service, 'invoke_setup_checkout_session_api', return_value=fake_session) as mock_invoke_setup, \
             patch.object(stripe_service, 'invoke_get_checkout_session_status_api') as mock_invoke_status:
            result = stripe_service.create_setup_checkout_session(self.test_uuid)

        self.assertEqual(result, fake_session)
        mock_invoke_setup.assert_called_once_with(self.test_uuid)
        mock_invoke_status.assert_not_called()

        saved_payment = stripe_service.get_current_payment_state(self.test_uuid)
        self.assertEqual(saved_payment.get('payment_setup_status'), ecwp.PaymentSetupStatus.WAITING_FOR_USER.value)
        self.assertEqual(saved_payment.get('pending_setup_session'), fake_session)

    def test_invoke_setup_checkout_session_api_raises_when_success_url_missing(self):
        with patch.object(stripe_service, 'STRIPE_SUCCESS_URL', None), \
             patch.object(stripe_service.stripe.checkout.Session, 'create') as mock_create:
            with self.assertRaisesRegex(ValueError, 'STRIPE_SUCCESS_URL is required for setup checkout'):
                stripe_service.invoke_setup_checkout_session_api(self.test_uuid)

        mock_create.assert_not_called()

    def test_create_setup_checkout_session_returns_existing_open_pending_session(self):
        pending_session = {
            'id': 'cs_setup_open_123',
            'url': 'https://checkout.stripe.com/c/pay/cs_setup_open_123',
            'status': 'open',
        }
        payment_db = stripe_service.esas.StateStorage.get_state_storage(self.test_uuid)
        seed_payment = ecwp.Payment()
        seed_payment.payment_setup_status = ecwp.PaymentSetupStatus.WAITING_FOR_USER
        seed_payment.pending_setup_session = {'id': pending_session['id']}
        payment_db.upsert_state(
            stripe_service.esas.StateName.PAYMENT,
            seed_payment,
        )

        with patch.object(stripe_service, 'invoke_get_checkout_session_status_api', return_value=pending_session) as mock_invoke_status, \
             patch.object(stripe_service, 'invoke_setup_checkout_session_api') as mock_invoke_setup:
            result = stripe_service.create_setup_checkout_session(self.test_uuid)

        self.assertEqual(result, pending_session)
        mock_invoke_status.assert_called_once_with(self.test_uuid)
        mock_invoke_setup.assert_not_called()

    def test_invoke_setup_checkout_session_api_propagates_stripe_error(self):
        with patch.object(stripe_service, 'STRIPE_SUCCESS_URL', 'https://example.com/success'), \
             patch.object(stripe_service.stripe.checkout.Session, 'create', side_effect=RuntimeError('stripe request failed')):
            with self.assertRaisesRegex(RuntimeError, 'stripe request failed'):
                stripe_service.invoke_setup_checkout_session_api(self.test_uuid)


if __name__ == '__main__':
    unittest.main()
