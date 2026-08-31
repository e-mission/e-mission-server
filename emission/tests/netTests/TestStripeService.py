from builtins import *
import json
import os
import unittest
from unittest.mock import patch

import emission.core.wrapper.user as ecwu
import emission.core.wrapper.payment as ecwp

os.environ.setdefault('STRIPE_SECRET_KEY', 'sk_test_dummy')

import emission.net.ext_service.stripe.stripe_service as stripe_service


class TestStripeService(unittest.TestCase):
    def setUp(self):
        self.test_email = f"stripe-{self._testMethodName}@example.com"
        self.test_uuid = ecwu.User.register(self.test_email).uuid
        self._sandbox_patcher = patch.object(stripe_service, 'STRIPE_IS_SANDBOX', True)
        self._sandbox_patcher.start()

    def tearDown(self):
        self._sandbox_patcher.stop()
        ecwu.User.unregister(self.test_email)

    def test_create_setup_checkout_session_success(self):
        fake_customer = {
            'id': 'cus_123',
        }
        fake_session = {
            'id': 'cs_setup_123',
            'url': 'https://checkout.stripe.com/c/pay/cs_setup_123',
            'status': 'open',
        }

        mock_user = unittest.mock.Mock()
        mock_user.create_and_store_username.return_value = 'atlas_beacon'

        with patch.object(stripe_service.User, 'fromUUID', return_value=mock_user) as mock_from_uuid, \
             patch.object(stripe_service.stripe.Customer, 'create', return_value=json.dumps(fake_customer)) as mock_create_customer, \
             patch.object(stripe_service, 'invoke_setup_checkout_session_api', return_value=fake_session) as mock_invoke_setup, \
             patch.object(stripe_service, 'invoke_get_checkout_session_status_api') as mock_invoke_status:
            result = stripe_service.create_setup_checkout_session(self.test_uuid)

        self.assertEqual(result, fake_session)
        mock_from_uuid.assert_called_once_with(self.test_uuid)
        mock_user.create_and_store_username.assert_called_once_with()
        mock_create_customer.assert_called_once_with(
            metadata={'username': 'atlas_beacon'},
            description='atlas_beacon',
        )
        mock_invoke_setup.assert_called_once_with(self.test_uuid, 'cus_123')
        mock_invoke_status.assert_not_called()

        saved_payment = stripe_service.get_current_payment_state(self.test_uuid)
        self.assertEqual(saved_payment.get('payment_setup_status'), ecwp.PaymentSetupStatus.WAITING_FOR_USER.value)
        self.assertEqual(saved_payment.get('pending_setup_session'), fake_session)
        self.assertEqual(saved_payment.get('stripe_customer_id'), 'cus_123')

    def test_invoke_setup_checkout_session_api_raises_when_success_url_missing(self):
        with patch.object(stripe_service, 'STRIPE_SUCCESS_URL', None), \
             patch.object(stripe_service.stripe.checkout.Session, 'create') as mock_create:
            with self.assertRaisesRegex(ValueError, 'STRIPE_SUCCESS_URL is required for setup checkout'):
                stripe_service.invoke_setup_checkout_session_api(self.test_uuid, 'cus_123')

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

        mock_user = unittest.mock.Mock()
        mock_user.create_and_store_username.return_value = 'atlas_beacon'

        with patch.object(stripe_service.User, 'fromUUID', return_value=mock_user) as mock_from_uuid, \
             patch.object(stripe_service, 'invoke_get_checkout_session_status_api', return_value=pending_session) as mock_invoke_status, \
             patch.object(stripe_service.stripe.Customer, 'create', return_value=json.dumps({'id': 'cus_123'})) as mock_create_customer, \
             patch.object(stripe_service, 'invoke_setup_checkout_session_api') as mock_invoke_setup:
            result = stripe_service.create_setup_checkout_session(self.test_uuid)

        self.assertEqual(result, pending_session)
        mock_invoke_status.assert_called_once_with(self.test_uuid)
        mock_from_uuid.assert_called_once_with(self.test_uuid)
        mock_user.create_and_store_username.assert_called_once_with()
        mock_create_customer.assert_called_once_with(
            metadata={'username': 'atlas_beacon'},
            description='atlas_beacon',
        )
        mock_invoke_setup.assert_not_called()

    def test_create_setup_checkout_session_reuses_saved_customer(self):
        payment_db = stripe_service.esas.StateStorage.get_state_storage(self.test_uuid)
        seed_payment = ecwp.Payment()
        seed_payment.payment_setup_status = ecwp.PaymentSetupStatus.NOT_STARTED
        seed_payment.stripe_customer_id = 'cus_saved_123'
        payment_db.upsert_state(
            stripe_service.esas.StateName.PAYMENT,
            seed_payment,
        )

        fake_session = {
            'id': 'cs_setup_123',
            'url': 'https://checkout.stripe.com/c/pay/cs_setup_123',
            'status': 'open',
        }

        with patch.object(stripe_service.User, 'fromUUID') as mock_from_uuid, \
             patch.object(stripe_service.stripe.Customer, 'create') as mock_create_customer, \
             patch.object(stripe_service, 'invoke_setup_checkout_session_api', return_value=fake_session) as mock_invoke_setup:
            result = stripe_service.create_setup_checkout_session(self.test_uuid)

        self.assertEqual(result, fake_session)
        mock_from_uuid.assert_not_called()
        mock_create_customer.assert_not_called()
        mock_invoke_setup.assert_called_once_with(self.test_uuid, 'cus_saved_123')

    def test_invoke_setup_checkout_session_api_propagates_stripe_error(self):
        with patch.object(stripe_service, 'STRIPE_SUCCESS_URL', 'https://example.com/success'), \
             patch.object(stripe_service.stripe.checkout.Session, 'create', side_effect=RuntimeError('stripe request failed')):
            with self.assertRaisesRegex(RuntimeError, 'stripe request failed'):
                stripe_service.invoke_setup_checkout_session_api(self.test_uuid, 'cus_123')

    def test_create_hold_payment_intent_uses_saved_setup_payment_method(self):
        payment_db = stripe_service.esas.StateStorage.get_state_storage(self.test_uuid)
        seed_payment = ecwp.Payment()
        seed_payment.payment_setup_status = ecwp.PaymentSetupStatus.SUCCEEDED
        seed_payment.stripe_customer_id = 'cus_saved_123'
        seed_payment.payment_setup = {
            'id': 'seti_123',
            'payment_method': 'pm_saved_123',
        }
        payment_db.upsert_state(
            stripe_service.esas.StateName.PAYMENT,
            seed_payment,
        )

        fake_intent = {
            'id': 'pi_hold_123',
            'status': 'requires_capture',
            'amount': 1200,
            'capture_method': 'manual',
            'payment_method': 'pm_saved_123',
        }

        with patch.object(stripe_service.stripe.PaymentIntent, 'create', return_value=json.dumps(fake_intent)) as mock_create:
            result = stripe_service.create_hold_payment_intent(self.test_uuid, 1200, metadata={'vehicle_id': 'bike-123'})

        self.assertEqual(result, fake_intent)
        mock_create.assert_called_once()
        called_kwargs = mock_create.call_args.kwargs
        self.assertEqual(called_kwargs['amount'], 1200)
        self.assertEqual(called_kwargs['currency'], 'usd')
        self.assertEqual(called_kwargs['payment_method'], 'pm_saved_123')
        self.assertEqual(called_kwargs['customer'], 'cus_saved_123')
        self.assertEqual(called_kwargs['capture_method'], 'manual')
        self.assertTrue(called_kwargs['confirm'])
        self.assertTrue(called_kwargs['off_session'])
        self.assertEqual(called_kwargs['metadata']['vehicle_id'], 'bike-123')

    def test_check_pending_setup_status_attaches_payment_method_to_customer(self):
        payment_db = stripe_service.esas.StateStorage.get_state_storage(self.test_uuid)
        seed_payment = ecwp.Payment()
        seed_payment.payment_setup_status = ecwp.PaymentSetupStatus.WAITING_FOR_USER
        seed_payment.pending_setup_session = {'id': 'cs_setup_123'}
        seed_payment.stripe_customer_id = 'cus_saved_123'
        payment_db.upsert_state(
            stripe_service.esas.StateName.PAYMENT,
            seed_payment,
        )

        complete_session = {
            'id': 'cs_setup_123',
            'status': 'complete',
            'setup_intent': {
                'id': 'seti_123',
                'payment_method': 'pm_saved_123',
            },
        }

        with patch.object(stripe_service, 'invoke_get_checkout_session_status_api', return_value=complete_session), \
             patch.object(stripe_service.stripe.PaymentMethod, 'attach', return_value=json.dumps({'id': 'pm_saved_123'})) as mock_attach:
            result = stripe_service.check_pending_setup_status(self.test_uuid)

        self.assertEqual(result, ecwp.PaymentSetupStatus.SUCCEEDED)
        mock_attach.assert_called_once_with('pm_saved_123', customer='cus_saved_123')

        saved_payment = stripe_service.get_current_payment_state(self.test_uuid)
        self.assertEqual(saved_payment.get('stripe_customer_id'), 'cus_saved_123')
        self.assertEqual(saved_payment.get('payment_setup_status'), ecwp.PaymentSetupStatus.SUCCEEDED.value)

    def test_create_hold_payment_intent_requires_succeeded_setup(self):
        payment_db = stripe_service.esas.StateStorage.get_state_storage(self.test_uuid)
        seed_payment = ecwp.Payment()
        seed_payment.payment_setup_status = ecwp.PaymentSetupStatus.WAITING_FOR_USER
        seed_payment.payment_setup = {'payment_method': 'pm_saved_123'}
        payment_db.upsert_state(
            stripe_service.esas.StateName.PAYMENT,
            seed_payment,
        )

        with self.assertRaisesRegex(ValueError, 'Payment setup is not complete'):
            stripe_service.create_hold_payment_intent(self.test_uuid, 1200)

    def test_capture_hold_payment_intent_calls_stripe_capture(self):
        fake_capture = {
            'id': 'pi_hold_123',
            'status': 'succeeded',
            'amount_capturable': 0,
        }

        with patch.object(stripe_service.stripe.PaymentIntent, 'capture', return_value=json.dumps(fake_capture)) as mock_capture:
            result = stripe_service.capture_hold_payment_intent('pi_hold_123', amount_to_capture_cents=900)

        self.assertEqual(result, fake_capture)
        mock_capture.assert_called_once_with('pi_hold_123', amount_to_capture=900)

    def test_cancel_hold_payment_intent_calls_stripe_cancel(self):
        fake_cancel = {
            'id': 'pi_hold_123',
            'status': 'canceled',
        }

        with patch.object(stripe_service.stripe.PaymentIntent, 'cancel', return_value=json.dumps(fake_cancel)) as mock_cancel:
            result = stripe_service.cancel_hold_payment_intent('pi_hold_123')

        self.assertEqual(result, fake_cancel)
        mock_cancel.assert_called_once_with('pi_hold_123')

    def test_cancel_hold_payment_intent_raises_on_missing_id(self):
        with patch.object(stripe_service.stripe.PaymentIntent, 'cancel') as mock_cancel:
            with self.assertRaisesRegex(ValueError, 'payment_intent_id is required'):
                stripe_service.cancel_hold_payment_intent(None)

        mock_cancel.assert_not_called()


if __name__ == '__main__':
    unittest.main()
