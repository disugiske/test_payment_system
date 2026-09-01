"""Outbox publisher: fetch_pending filters, publish_batch marks rows correctly."""
from __future__ import annotations

import uuid
from typing import Any
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import OutboxEvent, OutboxStatus
from app.database.queries.outbox import fetch_pending, mark_failed, mark_published
from app.outbox_publisher import publish_batch

pytestmark = pytest.mark.integration


async def _seed_event(
    session: AsyncSession, payload: dict[str, Any] | None = None
) -> uuid.UUID:
    event_id = uuid.uuid4()
    session.add(
        OutboxEvent(
            id=event_id,
            aggregate_id=uuid.uuid4(),
            event_type="payment.created",
            routing_key="payments.new",
            payload=payload or {"foo": "bar"},
        )
    )
    await session.commit()
    return event_id


async def test_fetch_pending_returns_only_pending(db_session: AsyncSession) -> None:
    await _seed_event(db_session)
    published_id = await _seed_event(db_session)
    await mark_published(db_session, [published_id])
    await db_session.commit()

    pending = await fetch_pending(db_session, limit=10)
    assert len(pending) == 1
    assert pending[0].id != published_id


async def test_mark_failed_increments_attempts(db_session: AsyncSession) -> None:
    event_id = await _seed_event(db_session)

    await mark_failed(db_session, [(event_id, "boom")])
    await db_session.commit()

    row = await db_session.scalar(
        select(OutboxEvent).where(OutboxEvent.id == event_id)
    )
    assert row.attempts == 1
    assert row.last_error == "boom"
    assert row.status == OutboxStatus.PENDING  # still pending — only attempts++


async def test_publish_batch_marks_published_on_success(
    db_session: AsyncSession,
) -> None:
    """Critical: successful publish flips status so events aren't sent twice."""
    await _seed_event(db_session)
    await _seed_event(db_session)

    broker = AsyncMock()
    broker.publish = AsyncMock(return_value=None)

    published = await publish_batch(db_session, broker)
    assert published == 2
    assert broker.publish.await_count == 2

    statuses = (await db_session.execute(select(OutboxEvent.status))).scalars().all()
    assert all(s == OutboxStatus.PUBLISHED for s in statuses)


async def test_publish_batch_records_failures(db_session: AsyncSession) -> None:
    """Broker errors must increment attempts, not flip to PUBLISHED."""
    event_id = await _seed_event(db_session)

    broker = AsyncMock()
    broker.publish = AsyncMock(side_effect=RuntimeError("rabbit down"))

    published = await publish_batch(db_session, broker)
    assert published == 0

    row = await db_session.scalar(
        select(OutboxEvent).where(OutboxEvent.id == event_id)
    )
    assert row.status == OutboxStatus.PENDING
    assert row.attempts == 1
    assert "rabbit down" in row.last_error


async def test_publish_batch_no_events_is_noop(db_session: AsyncSession) -> None:
    broker = AsyncMock()
    published = await publish_batch(db_session, broker)
    assert published == 0
    broker.publish.assert_not_awaited()
