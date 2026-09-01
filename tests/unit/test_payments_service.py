"""Payments business logic: outbox event payload shape, create_payment orchestration."""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock

import pytest

import app.services.payments as payments_service
from app.database import Currency, PaymentStatus
from app.database.dto import PaymentDTO
from app.schemas import PaymentCreate
from app.services.payments import _build_event_payload, _decimal_to_str, _enum_value, create_payment

pytestmark = pytest.mark.unit


def _make_payment(**overrides: Any) -> PaymentDTO:
    defaults: dict[str, Any] = dict(
        id=uuid.uuid4(),
        amount=Decimal("100.50"),
        currency=Currency.RUB,
        description="Order #1",
        payment_metadata={"order_id": 1},
        status=PaymentStatus.PENDING,
        idempotency_key="idem-1",
        webhook_url=None,
        created_at=datetime.now(timezone.utc),
        processed_at=None,
    )
    defaults.update(overrides)
    return PaymentDTO(**defaults)


def test_decimal_to_str_preserves_trailing_zeros() -> None:
    assert _decimal_to_str(Decimal("10.00")) == "10.00"


def test_enum_value_accepts_enum_or_plain_string() -> None:
    assert _enum_value(Currency.USD) == "USD"
    assert _enum_value("EUR") == "EUR"


def test_build_event_payload_serializes_decimal_and_currency() -> None:
    payment = _make_payment(amount=Decimal("100.50"), currency=Currency.RUB)

    payload = _build_event_payload(payment, "idem-1")

    assert payload["payment_id"] == str(payment.id)
    assert payload["amount"] == "100.50"
    assert payload["currency"] == "RUB"
    assert payload["metadata"] == payment.payment_metadata
    assert payload["idempotency_key"] == "idem-1"


async def test_create_payment_returns_existing_on_idempotency_key_match(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    existing = _make_payment(idempotency_key="dup")
    monkeypatch.setattr(
        payments_service, "get_by_idempotency_key", AsyncMock(return_value=existing)
    )
    insert_payment_mock = AsyncMock()
    monkeypatch.setattr(payments_service, "insert_payment", insert_payment_mock)
    session = AsyncMock()
    data = PaymentCreate(amount=Decimal("5"), currency=Currency.USD)

    payment, created = await create_payment(session, data, "dup")

    assert payment is existing
    assert created is False
    insert_payment_mock.assert_not_awaited()
    session.commit.assert_not_awaited()


async def test_create_payment_inserts_payment_and_outbox_event_then_commits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Critical: outbox event must be written before commit, in the same transaction."""
    monkeypatch.setattr(
        payments_service, "get_by_idempotency_key", AsyncMock(return_value=None)
    )
    new_payment = _make_payment(idempotency_key="new-1")
    monkeypatch.setattr(
        payments_service, "insert_payment", AsyncMock(return_value=new_payment)
    )
    insert_event_mock = AsyncMock()
    monkeypatch.setattr(payments_service, "insert_event", insert_event_mock)
    session = AsyncMock()
    data = PaymentCreate(
        amount=Decimal("5"), currency=Currency.USD, webhook_url="https://example.com/hook"
    )

    payment, created = await create_payment(session, data, "new-1")

    assert payment is new_payment
    assert created is True
    insert_event_mock.assert_awaited_once()
    _, kwargs = insert_event_mock.await_args
    assert kwargs["aggregate_id"] == new_payment.id
    assert kwargs["event_type"] == "payment.created"
    session.commit.assert_awaited_once()
