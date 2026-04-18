"""Business logic for payments.

Orchestrates DB queries and outbox publishing within a single transaction.
No raw SQL here.
"""
from decimal import Decimal
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.database import Currency
from app.database.dto import PaymentDTO
from app.database.queries.outbox import insert_event
from app.database.queries.payments import get_by_idempotency_key, insert_payment
from app.schemas import PaymentCreate

settings = get_settings()


async def create_payment(
    session: AsyncSession,
    data: PaymentCreate,
    idempotency_key: str,
) -> tuple[PaymentDTO, bool]:
    """Create a payment using the outbox pattern.

    Returns ``(payment, created)``; ``created`` is ``False`` if an existing
    payment was found by idempotency key.
    """
    existing = await get_by_idempotency_key(session, idempotency_key)
    if existing is not None:
        return existing, False

    payment = await insert_payment(
        session,
        amount=data.amount,
        currency=data.currency,
        description=data.description,
        metadata=data.metadata,
        idempotency_key=idempotency_key,
        webhook_url=str(data.webhook_url) if data.webhook_url else None,
    )

    await insert_event(
        session,
        aggregate_id=payment.id,
        event_type="payment.created",
        routing_key=settings.payments_new_queue,
        payload=_build_event_payload(payment, idempotency_key),
    )

    await session.commit()
    return payment, True


def _build_event_payload(payment: PaymentDTO, idempotency_key: str) -> dict[str, Any]:
    return {
        "payment_id": str(payment.id),
        "amount": _decimal_to_str(payment.amount),
        "currency": _enum_value(payment.currency),
        "description": payment.description,
        "metadata": payment.payment_metadata,
        "webhook_url": payment.webhook_url,
        "idempotency_key": idempotency_key,
    }


def _decimal_to_str(value: Decimal) -> str:
    return format(value, "f")


def _enum_value(value: Currency | str) -> str:
    return value.value if isinstance(value, Currency) else value
