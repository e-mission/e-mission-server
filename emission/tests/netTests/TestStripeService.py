from builtins import *
import unittest
from unittest.mock import patch
from types import SimpleNamespace

import emission.core.wrapper.user as ecwu
import emission.net.ext_service.stripe.stripe_service as stripe_service


class TestStripeService(unittest.TestCase):
    def setUp(self):
        self.test_email = f"stripe-{self._testMethodName}@example.com"
        self.test_uuid = ecwu.User.register(self.test_email).uuid

    def tearDown(self):
        ecwu.User.unregister(self.test_email)

    def test_create_setup_checkout_session_success(self):
        fake_session = SimpleNamespace(
            id='cs_setup_123',
            url='https://checkout.stripe.com/c/pay/cs_setup_123',
        )

        with patch.object(stripe_service, 'STRIPE_SUCCESS_URL', 'https://example.com/success'), \
             patch.object(stripe_service, 'STRIPE_CANCEL_URL', 'https://example.com/cancel'), \
             patch.object(stripe_service.stripe.checkout.Session, 'create', return_value=fake_session) as mock_create:
            result = stripe_service.create_setup_checkout_session(self.test_uuid)

        self.assertEqual(result, {
            'id': 'cs_setup_123',
            'url': 'https://checkout.stripe.com/c/pay/cs_setup_123',
        })
        mock_create.assert_called_once_with(
            mode='setup',
            success_url='https://example.com/success',
            currency='USD',
            cancel_url='https://example.com/cancel',
        )

    def test_create_setup_checkout_session_raises_when_success_url_missing(self):
        with patch.object(stripe_service, 'STRIPE_SUCCESS_URL', None), \
             patch.object(stripe_service.stripe.checkout.Session, 'create') as mock_create:
            with self.assertRaisesRegex(ValueError, 'STRIPE_SUCCESS_URL is required for setup checkout'):
                stripe_service.create_setup_checkout_session(self.test_uuid)

        mock_create.assert_not_called()

    def test_create_setup_checkout_session_raises_when_stripe_response_invalid(self):
        invalid_session = SimpleNamespace(id='cs_setup_123', url=None)

        with patch.object(stripe_service, 'STRIPE_SUCCESS_URL', 'https://example.com/success'), \
             patch.object(stripe_service, 'STRIPE_CANCEL_URL', None), \
             patch.object(stripe_service.stripe.checkout.Session, 'create', return_value=invalid_session) as mock_create:
            with self.assertRaisesRegex(ValueError, 'Invalid Stripe setup checkout response: missing id or url'):
                stripe_service.create_setup_checkout_session(self.test_uuid)

        mock_create.assert_called_once_with(
            mode='setup',
            success_url='https://example.com/success',
            currency='USD',
        )

    def test_create_setup_checkout_session_propagates_stripe_error(self):
        with patch.object(stripe_service, 'STRIPE_SUCCESS_URL', 'https://example.com/success'), \
             patch.object(stripe_service.stripe.checkout.Session, 'create', side_effect=RuntimeError('stripe request failed')):
            with self.assertRaisesRegex(RuntimeError, 'stripe request failed'):
                stripe_service.create_setup_checkout_session(self.test_uuid)


if __name__ == '__main__':
    unittest.main()
