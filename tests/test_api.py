"""HTTP API: auth, validation, idempotency, full create→get flow."""
from __future__ import annotations

from typing import Any

from httpx import AsyncClient
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import OutboxEvent, OutboxStatus, Payment, PaymentStatus

VALID_BODY: dict[str, Any] = {
    "amount": "100.50",
    "currency": "RUB",
    "description": "Order #1",
    "metadata": {"order_id": 1},
    "webhook_url": "https://example.com/hook",
}


async def test_post_requires_api_key(api_client: AsyncClient) -> None:
    api_client.headers.pop("X-API-Key")
    r = await api_client.post(
        "/api/v1/payments",
        json=VALID_BODY,
        headers={"Idempotency-Key": "k1"},
    )
    assert r.status_code == 401


async def test_post_requires_idempotency_key(api_client: AsyncClient) -> None:
    r = await api_client.post("/api/v1/payments", json=VALID_BODY)
    assert r.status_code == 422


async def test_post_rejects_invalid_amount(api_client: AsyncClient) -> None:
    bad = {**VALID_BODY, "amount": "-10"}
    r = await api_client.post(
        "/api/v1/payments", json=bad, headers={"Idempotency-Key": "k1"}
    )
    assert r.status_code == 422


async def test_post_creates_payment_and_outbox_in_one_tx(
    api_client: AsyncClient,
    db_session: AsyncSession,
) -> None:
    """Critical: outbox pattern guarantee — both rows must be present."""
    r = await api_client.post(
        "/api/v1/payments",
        json=VALID_BODY,
        headers={"Idempotency-Key": "order-1"},
    )
    assert r.status_code == 202
    body = r.json()
    assert body["status"] == PaymentStatus.PENDING.value
    payment_id = body["payment_id"]

    payment = await db_session.scalar(
        select(Payment).where(Payment.id == payment_id)
    )
    assert payment is not None
    assert payment.idempotency_key == "order-1"

    outbox = await db_session.scalar(
        select(OutboxEvent).where(OutboxEvent.aggregate_id == payment.id)
    )
    assert outbox is not None
    assert outbox.status == OutboxStatus.PENDING
    assert outbox.event_type == "payment.created"
    assert outbox.payload["payment_id"] == payment_id
    assert outbox.payload["idempotency_key"] == "order-1"


async def test_post_is_idempotent(
    api_client: AsyncClient,
    db_session: AsyncSession,
) -> None:
    """Same Idempotency-Key → same payment, no duplicates, status 200."""
    headers = {"Idempotency-Key": "order-2"}

    r1 = await api_client.post("/api/v1/payments", json=VALID_BODY, headers=headers)
    r2 = await api_client.post("/api/v1/payments", json=VALID_BODY, headers=headers)

    assert r1.status_code == 202
    assert r2.status_code == 200  # replay
    assert r1.json()["payment_id"] == r2.json()["payment_id"]

    count = await db_session.scalar(
        select(func.count()).select_from(Payment)
    )
    assert count == 1


async def test_get_returns_payment(api_client: AsyncClient) -> None:
    create = await api_client.post(
        "/api/v1/payments",
        json=VALID_BODY,
        headers={"Idempotency-Key": "order-3"},
    )
    payment_id = create.json()["payment_id"]

    r = await api_client.get(f"/api/v1/payments/{payment_id}")
    assert r.status_code == 200
    body = r.json()
    assert body["id"] == payment_id
    assert body["amount"] == "100.50"
    assert body["currency"] == "RUB"
    # FastAPI serializes by alias (default response_model_by_alias=True),
    # so the field appears under its alias `payment_metadata`.
    assert body["payment_metadata"] == {"order_id": 1}
    assert body["status"] == "pending"


async def test_get_unknown_returns_404(api_client: AsyncClient) -> None:
    r = await api_client.get("/api/v1/payments/00000000-0000-0000-0000-000000000000")
    assert r.status_code == 404
