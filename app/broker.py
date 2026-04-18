"""RabbitMQ topology and broker factory.

Exchange / queue layout:

  payments (direct exchange)
    -> payments.new (queue, dead-letter -> payments.dlx)
  payments.dlx (direct exchange)
    -> payments.dlq (queue)
"""
from faststream.rabbit import (
    ExchangeType,
    RabbitBroker,
    RabbitExchange,
    RabbitQueue,
)

from app.config import get_settings

settings = get_settings()

payments_exchange = RabbitExchange(
    settings.payments_exchange,
    type=ExchangeType.DIRECT,
    durable=True,
)

payments_dlx = RabbitExchange(
    settings.payments_dlx,
    type=ExchangeType.DIRECT,
    durable=True,
)

payments_dlq = RabbitQueue(
    settings.payments_dlq,
    durable=True,
    routing_key=settings.payments_dlq,
)

payments_new_queue = RabbitQueue(
    settings.payments_new_queue,
    durable=True,
    routing_key=settings.payments_new_queue,
    arguments={
        "x-dead-letter-exchange": settings.payments_dlx,
        "x-dead-letter-routing-key": settings.payments_dlq,
    },
)


def build_broker() -> RabbitBroker:
    return RabbitBroker(settings.rabbitmq_url)


async def declare_topology(broker: RabbitBroker) -> None:
    """Declare exchanges, queues and bindings. Safe to call multiple times.

    Bindings (queue ↔ exchange) are created here so that messages from the
    publisher are routed correctly even if the consumer hasn't started yet.
    """
    await broker.connect()

    new_exchange = await broker.declare_exchange(payments_exchange)
    dlx_exchange = await broker.declare_exchange(payments_dlx)
    new_queue = await broker.declare_queue(payments_new_queue)
    dlq_queue = await broker.declare_queue(payments_dlq)

    await new_queue.bind(new_exchange, routing_key=settings.payments_new_queue)
    await dlq_queue.bind(dlx_exchange, routing_key=settings.payments_dlq)
