"""Payment consumer: gateway/webhook orchestration, retry-until-DLQ behavior."""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock

import pytest
from faststream.exceptions import RejectMessage

import app.consumer as consumer
from app.consumer import GatewayError, _process_once, _process_with_retry, handle_dlq, handle_new_payment
from app.database import Currency, PaymentStatus
from app.database.dto import PaymentDTO

pytestmark = pytest.mark.unit


def _make_payment(**overrides: Any) -> PaymentDTO:
    defaults: dict[str, Any] = dict(
        id=uuid.uuid4(),
        amount=Decimal("10.00"),
        currency=Currency.USD,
        description="",
        payment_metadata={},
        status=PaymentStatus.SUCCEEDED,
        idempotency_key="idem",
        webhook_url=None,
        created_at=datetime.now(timezone.utc),
        processed_at=datetime.now(timezone.utc),
    )
    defaults.update(overrides)
    return PaymentDTO(**defaults)


@pytest.fixture(autouse=True)
def _fast_retries(monkeypatch: pytest.MonkeyPatch) -> None:
    """Disable tenacity backoff sleeps so retry tests stay snappy."""
    monkeypatch.setattr(consumer, "wait_exponential", lambda **_: lambda *_a, **_k: 0)


async def test_process_once_sends_webhook_when_url_present(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payment = _make_payment(webhook_url="https://example.com/hook")
    monkeypatch.setattr(consumer, "call_gateway", AsyncMock(return_value=True))
    monkeypatch.setattr(consumer, "_update_status", AsyncMock(return_value=payment))
    send_webhook_mock = AsyncMock()
    monkeypatch.setattr(consumer, "send_webhook", send_webhook_mock)

    await _process_once(payment.id)

    send_webhook_mock.assert_awaited_once()
    url, payload = send_webhook_mock.await_args.args
    assert url == "https://example.com/hook"
    assert payload["payment_id"] == str(payment.id)
    assert payload["amount"] == "10.00"
    assert payload["currency"] == "USD"


async def test_process_once_skips_webhook_when_url_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payment = _make_payment(webhook_url=None)
    monkeypatch.setattr(consumer, "call_gateway", AsyncMock(return_value=True))
    monkeypatch.setattr(consumer, "_update_status", AsyncMock(return_value=payment))
    send_webhook_mock = AsyncMock()
    monkeypatch.setattr(consumer, "send_webhook", send_webhook_mock)

    await _process_once(payment.id)

    send_webhook_mock.assert_not_awaited()


async def test_process_once_raises_gateway_error_without_calling_webhook(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payment = _make_payment(webhook_url="https://example.com/hook", status=PaymentStatus.FAILED)
    monkeypatch.setattr(consumer, "call_gateway", AsyncMock(return_value=False))
    monkeypatch.setattr(consumer, "_update_status", AsyncMock(return_value=payment))
    send_webhook_mock = AsyncMock()
    monkeypatch.setattr(consumer, "send_webhook", send_webhook_mock)

    with pytest.raises(GatewayError):
        await _process_once(payment.id)

    send_webhook_mock.assert_not_awaited()


async def test_process_once_returns_quietly_when_payment_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(consumer, "call_gateway", AsyncMock(return_value=True))
    monkeypatch.setattr(consumer, "_update_status", AsyncMock(return_value=None))
    send_webhook_mock = AsyncMock()
    monkeypatch.setattr(consumer, "send_webhook", send_webhook_mock)

    await _process_once(uuid.uuid4())  # must not raise

    send_webhook_mock.assert_not_awaited()


async def test_process_with_retry_succeeds_after_transient_gateway_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payment = _make_payment(webhook_url=None)
    call_gateway_mock = AsyncMock(side_effect=[False, True])
    monkeypatch.setattr(consumer, "call_gateway", call_gateway_mock)
    monkeypatch.setattr(consumer, "_update_status", AsyncMock(return_value=payment))
    monkeypatch.setattr(consumer.settings, "consumer_max_attempts", 3)

    await _process_with_retry(payment.id)  # must not raise

    assert call_gateway_mock.await_count == 2


async def test_process_with_retry_reraises_after_exhausting_attempts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payment = _make_payment(webhook_url=None, status=PaymentStatus.FAILED)
    call_gateway_mock = AsyncMock(return_value=False)
    monkeypatch.setattr(consumer, "call_gateway", call_gateway_mock)
    monkeypatch.setattr(consumer, "_update_status", AsyncMock(return_value=payment))
    monkeypatch.setattr(consumer.settings, "consumer_max_attempts", 2)

    with pytest.raises(GatewayError):
        await _process_with_retry(payment.id)

    assert call_gateway_mock.await_count == 2


async def test_handle_new_payment_succeeds_without_raising(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payment = _make_payment(webhook_url=None)
    monkeypatch.setattr(consumer, "call_gateway", AsyncMock(return_value=True))
    monkeypatch.setattr(consumer, "_update_status", AsyncMock(return_value=payment))
    monkeypatch.setattr(consumer.settings, "consumer_max_attempts", 3)

    await handle_new_payment({"payment_id": str(payment.id)})  # must not raise


async def test_handle_new_payment_rejects_message_after_exhausting_retries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Critical: exhausted retries must dead-letter the message, not swallow the error."""
    payment = _make_payment(webhook_url=None, status=PaymentStatus.FAILED)
    monkeypatch.setattr(consumer, "call_gateway", AsyncMock(return_value=False))
    monkeypatch.setattr(consumer, "_update_status", AsyncMock(return_value=payment))
    monkeypatch.setattr(consumer.settings, "consumer_max_attempts", 1)

    with pytest.raises(RejectMessage):
        await handle_new_payment({"payment_id": str(payment.id)})


async def test_handle_dlq_does_not_raise() -> None:
    await handle_dlq({"payment_id": str(uuid.uuid4()), "reason": "boom"})
