"""Data-access functions for the `outbox` table."""
import uuid
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import insert, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import OutboxEvent, OutboxStatus
from app.database.dto import OutboxEventDTO

OUTBOX_COLUMNS = (
    OutboxEvent.id,
    OutboxEvent.aggregate_id,
    OutboxEvent.event_type,
    OutboxEvent.payload,
    OutboxEvent.routing_key,
    OutboxEvent.attempts,
)


async def insert_event(
    session: AsyncSession,
    *,
    aggregate_id: uuid.UUID,
    event_type: str,
    routing_key: str,
    payload: dict[str, Any],
) -> OutboxEventDTO:
    stmt = (
        insert(OutboxEvent)
        .values(
            aggregate_id=aggregate_id,
            event_type=event_type,
            routing_key=routing_key,
            payload=payload,
        )
        .returning(*OUTBOX_COLUMNS)
    )
    row = (await session.execute(stmt)).one()
    return OutboxEventDTO.model_validate(row)


async def fetch_pending(
    session: AsyncSession,
        limit: int,
) -> list[OutboxEventDTO]:
    stmt = (
        select(*OUTBOX_COLUMNS)
        .select_from(OutboxEvent)
        .where(OutboxEvent.status == OutboxStatus.PENDING)
        .order_by(OutboxEvent.created_at)
        .limit(limit)
        .with_for_update(skip_locked=True, of=OutboxEvent)
    )
    rows = (await session.execute(stmt)).all()
    return [OutboxEventDTO.model_validate(row) for row in rows]


async def mark_published(
    session: AsyncSession,
    event_ids: list[uuid.UUID],
) -> None:
    if not event_ids:
        return
    stmt = (
        update(OutboxEvent)
        .where(OutboxEvent.id.in_(event_ids))
        .values(
            status=OutboxStatus.PUBLISHED,
            published_at=datetime.now(timezone.utc),
        )
    )
    await session.execute(stmt)


async def mark_failed(
    session: AsyncSession,
    failures: list[tuple[uuid.UUID, str]],
) -> None:
    for event_id, error in failures:
        stmt = (
            update(OutboxEvent)
            .where(OutboxEvent.id == event_id)
            .values(
                attempts=OutboxEvent.attempts + 1,
                last_error=error,
            )
        )
        await session.execute(stmt)
