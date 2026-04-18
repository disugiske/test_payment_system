"""Outbox publisher.

Periodically polls the `outbox` table for PENDING events and publishes them
to RabbitMQ. SQL lives in ``app.database.queries.outbox``; this module is
pure orchestration: poll → publish → mark.
"""
import asyncio
import json
import logging
import uuid

from faststream.rabbit import RabbitBroker
from sqlalchemy.ext.asyncio import AsyncSession

from app.broker import (
    build_broker,
    declare_topology,
    payments_exchange,
)
from app.config import get_settings
from app.database import async_session_factory
from app.database.queries.outbox import fetch_pending, mark_failed, mark_published
from app.logging_config import setup_logging

logger = logging.getLogger(__name__)
settings = get_settings()


async def publish_batch(session: AsyncSession, broker: RabbitBroker) -> int:
    events = await fetch_pending(session, settings.outbox_batch_size)
    if not events:
        return 0

    succeeded_ids: list[uuid.UUID] = []
    failures: list[tuple[uuid.UUID, str]] = []

    for event in events:
        try:
            await broker.publish(
                json.dumps(event.payload).encode(),
                exchange=payments_exchange,
                routing_key=event.routing_key,
                content_type="application/json",
                headers={"event_type": event.event_type},
                persist=True,
            )
            succeeded_ids.append(event.id)
            logger.info(
                "outbox event published",
                extra={
                    "event_id": str(event.id),
                    "aggregate_id": str(event.aggregate_id),
                    "event_type": event.event_type,
                },
            )
        except Exception as exc:  # noqa: BLE001
            failures.append((event.id, str(exc)))
            logger.exception(
                "failed to publish outbox event",
                extra={"event_id": str(event.id), "attempts": event.attempts + 1},
            )

    await mark_published(session, succeeded_ids)
    await mark_failed(session, failures)
    await session.commit()
    return len(succeeded_ids)


async def run() -> None:
    setup_logging()
    broker = build_broker()
    await broker.connect()
    await declare_topology(broker)
    logger.info("outbox publisher started")

    try:
        while True:
            try:
                async with async_session_factory() as session:
                    published = await publish_batch(session, broker)
                if published == 0:
                    await asyncio.sleep(settings.outbox_poll_interval)
            except Exception:
                logger.exception("outbox publisher iteration failed")
                await asyncio.sleep(settings.outbox_poll_interval)
    finally:
        await broker.stop()


if __name__ == "__main__":
    asyncio.run(run())
