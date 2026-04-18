"""Payment processing consumer.

Single consumer that:
1. Reads messages from `payments.new`.
2. Simulates external gateway processing (2-5s, 90% success).
3. Updates the payment status in the DB.
4. Sends a webhook with retries and exponential backoff.
5. After `consumer_max_attempts` failures the message is dead-lettered
   to `payments.dlq` via RabbitMQ's `x-dead-letter-exchange` (the queue is
   configured with DLX in `app.broker`).
"""
import asyncio
import logging
import random
import uuid
from typing import Any

from faststream import FastStream
from faststream.exceptions import RejectMessage
from tenacity import (
    AsyncRetrying,
    before_sleep_log,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from app.broker import (
    build_broker,
    declare_topology,
    payments_dlq,
    payments_dlx,
    payments_exchange,
    payments_new_queue,
)
from app.config import get_settings
from app.database import Currency, PaymentStatus, async_session_factory
from app.database.dto import PaymentDTO
from app.database.queries.payments import update_status
from app.logging_config import setup_logging
from app.webhook import WebhookDeliveryError, send_webhook

logger = logging.getLogger(__name__)
settings = get_settings()

setup_logging()

broker = build_broker()
app = FastStream(broker)


@app.on_startup
async def on_startup() -> None:
    await declare_topology(broker)
    logger.info("consumer started")


class GatewayError(Exception):
    """Simulated transient error from the external payment gateway."""


async def call_gateway() -> bool:
    """Simulate external gateway: 2-5s latency, 90% success."""
    await asyncio.sleep(random.uniform(2, 5))
    return random.random() < 0.9


@broker.subscriber(payments_new_queue, payments_exchange)
async def handle_new_payment(body: dict[str, Any]) -> None:
    payment_id = uuid.UUID(body["payment_id"])
    logger.info("processing payment", extra={"payment_id": str(payment_id)})

    try:
        await _process_with_retry(payment_id)
    except Exception as exc:  # noqa: BLE001
        logger.error(
            "payment processing exhausted retries; sending to DLQ",
            extra={"payment_id": str(payment_id), "error": str(exc)},
        )
        # RejectMessage -> basic.nack(requeue=False) -> message dead-lettered
        # to `payments.dlx` -> `payments.dlq` (configured on the queue).
        raise RejectMessage() from exc


async def _process_with_retry(payment_id: uuid.UUID) -> None:
    """Run gateway + status update + webhook with up to N exponential retries."""
    async for attempt in AsyncRetrying(
        stop=stop_after_attempt(settings.consumer_max_attempts),
        wait=wait_exponential(multiplier=1, min=1, max=30),
        retry=retry_if_exception_type((GatewayError, WebhookDeliveryError)),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    ):
        with attempt:
            await _process_once(payment_id)


async def _process_once(payment_id: uuid.UUID) -> None:
    succeeded = await call_gateway()
    new_status = PaymentStatus.SUCCEEDED if succeeded else PaymentStatus.FAILED
    payment = await _update_status(payment_id, new_status)
    if payment is None:
        logger.error("payment not found in DB", extra={"payment_id": str(payment_id)})
        return

    if not succeeded:
        raise GatewayError("gateway reported failure")

    if payment.webhook_url:
        await send_webhook(
            payment.webhook_url,
            {
                "payment_id": str(payment.id),
                "status": payment.status.value
                if isinstance(payment.status, PaymentStatus)
                else payment.status,
                "amount": format(payment.amount, "f"),
                "currency": payment.currency.value
                if isinstance(payment.currency, Currency)
                else payment.currency,
                "processed_at": payment.processed_at.isoformat()
                if payment.processed_at
                else None,
            },
        )

    logger.info(
        "payment processed",
        extra={"payment_id": str(payment_id), "status": new_status.value},
    )


@broker.subscriber(payments_dlq, payments_dlx)
async def handle_dlq(body: dict[str, Any]) -> None:
    """Observability handler for messages that ended up in the DLQ."""
    logger.error("message moved to DLQ", extra={"payload": body})


async def _update_status(
    payment_id: uuid.UUID,
    status: PaymentStatus,
) -> PaymentDTO | None:
    async with async_session_factory() as session:
        row = await update_status(session, payment_id, status)
        await session.commit()
        return row
