import uuid

from fastapi import APIRouter, Depends, Header, HTTPException, Response, status
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth import verify_api_key
from app.database import get_session
from app.database.queries.payments import get_payment_by_id, get_by_idempotency_key
from app.schemas import PaymentAccepted, PaymentCreate, PaymentRead
from app.services.payments import create_payment

router = APIRouter(
    tags=["payments"],
    dependencies=[Depends(verify_api_key)],
)


@router.post(
    "/api/v1/payments",
    response_model=PaymentAccepted,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Create a new payment",
)
async def create_payment_handler(
    payload: PaymentCreate,
    response: Response,
    idempotency_key: str = Header(
        ...,
        alias="Idempotency-Key",
        min_length=1,
        max_length=255,
        description="Unique key to protect from duplicate requests",
    ),
    session: AsyncSession = Depends(get_session),
) -> PaymentAccepted:
    try:
        payment, created = await create_payment(
            session,
            payload,
            idempotency_key,
        )
    except IntegrityError:
        await session.rollback()
        existing = await get_by_idempotency_key(
            session,
            idempotency_key,
        )
        if existing is None:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Conflict on idempotency key",
            )
        payment, created = existing, False

    if not created:
        response.status_code = status.HTTP_200_OK

    return PaymentAccepted(
        payment_id=payment.id,
        status=payment.status,
        created_at=payment.created_at,
    )


@router.get(
    "/api/v1/payments/{payment_id}",
    response_model=PaymentRead,
    summary="Get payment information",
)
async def get_payment(
    payment_id: uuid.UUID,
    session: AsyncSession = Depends(get_session),
) -> PaymentRead:
    payment = await get_payment_by_id(session, payment_id)
    if payment is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Payment not found",
        )
    return PaymentRead.model_validate(payment)
