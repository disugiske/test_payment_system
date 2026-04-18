# Payments service

Асинхронный микросервис обработки платежей.

## Стек

- Python 3.12, FastAPI, Pydantic v2
- SQLAlchemy 2.0 (async) + PostgreSQL
- RabbitMQ + FastStream
- Alembic
- Docker / docker-compose

## Архитектура

```
┌──────────┐  HTTP  ┌──────┐  INSERT   ┌─────────────┐
│  client  │ ─────▶ │ API  │ ────────▶ │ payments +  │
└──────────┘        └──────┘ (1 tx)    │   outbox    │
                                        └──────┬──────┘
                                               │ poll
                                               ▼
                                        ┌─────────────┐
                                        │   outbox    │ ──▶ payments (exchange)
                                        │  publisher  │     └▶ payments.new (queue)
                                        └─────────────┘                │
                                                                       ▼
        ┌──────────────────────────────────────────────────────────────────┐
        │                         consumer                                 │
        │  gateway emulation 2-5s, 90% success → update DB → webhook       │
        │  retry x3 c экспоненциальной задержкой → reject → DLX → DLQ      │
        └──────────────────────────────────────────────────────────────────┘
```

### Гарантии

- **Outbox pattern.** При создании платежа в одной транзакции сохраняются
  и `payments`, и запись в `outbox`. Публикатор читает PENDING-события через
  `SELECT … FOR UPDATE SKIP LOCKED` и отмечает их PUBLISHED. Это даёт
  at-least-once доставку даже при падении API/брокера.
- **Идемпотентность.** Заголовок `Idempotency-Key` (обязательный). При
  повторном запросе возвращается ранее созданный платёж (HTTP 200).
  В БД на ключ навешен UNIQUE-индекс.
- **Retry.** В consumer'е: до 3 попыток обработки с экспоненциальной задержкой
  (1s → 2s → 4s, до 30s). Webhook доставляется через `httpx` + `tenacity` —
  до 3 попыток на 5xx и сетевые ошибки.
- **DLQ.** Очередь `payments.new` объявлена с `x-dead-letter-exchange =
  payments.dlx`. После исчерпания попыток consumer возвращает
  `RejectMessage` (basic.nack без requeue), и сообщение автоматически
  попадает в `payments.dlq`.
- **Аутентификация.** Все эндпоинты `/api/v1/*` защищены статическим API-ключом
  в заголовке `X-API-Key`.

## Запуск

```bash
cp .env.example .env
docker compose up --build
```

Сервисы:

| Сервис    | URL                                  |
|-----------|---------------------------------------|
| API       | http://localhost:8000                 |
| Swagger   | http://localhost:8000/docs            |
| RabbitMQ  | http://localhost:15672 (guest/guest)  |
| Postgres  | localhost:5432 (payments/payments)    |

Миграции применяются автоматически контейнером `migrate` перед стартом
остальных сервисов.

## API

### Создать платёж

```bash
curl -X POST http://localhost:8000/api/v1/payments \
  -H "X-API-Key: secret-api-key" \
  -H "Idempotency-Key: order-42" \
  -H "Content-Type: application/json" \
  -d '{
    "amount": "1000.50",
    "currency": "RUB",
    "description": "Order #42",
    "metadata": {"order_id": 42, "user_id": 7},
    "webhook_url": "https://webhook.site/your-id"
  }'
```

Ответ `202 Accepted`:

```json
{
  "payment_id": "8a9c1f0b-...",
  "status": "pending",
  "created_at": "2026-04-18T18:30:00+00:00"
}
```

Повторный запрос с тем же `Idempotency-Key` вернёт `200 OK` и тот же
`payment_id`.

### Получить платёж

```bash
curl http://localhost:8000/api/v1/payments/<payment_id> \
  -H "X-API-Key: secret-api-key"
```

```json
{
  "id": "8a9c1f0b-...",
  "amount": "1000.50",
  "currency": "RUB",
  "description": "Order #42",
  "metadata": {"order_id": 42, "user_id": 7},
  "status": "succeeded",
  "idempotency_key": "order-42",
  "webhook_url": "https://webhook.site/your-id",
  "created_at": "2026-04-18T18:30:00+00:00",
  "processed_at": "2026-04-18T18:30:04+00:00"
}
```

### Webhook payload

```json
{
  "payment_id": "8a9c1f0b-...",
  "status": "succeeded",
  "amount": "1000.50",
  "currency": "RUB",
  "processed_at": "2026-04-18T18:30:04+00:00"
}
```

## Топология RabbitMQ

| Объект                    | Тип            | Назначение                            |
|---------------------------|----------------|---------------------------------------|
| `payments`                | direct exch.   | Основной обмен для событий платежей   |
| `payments.new`            | queue          | Входящие новые платежи (DLX→`payments.dlx`) |
| `payments.dlx`            | direct exch.   | Dead-letter exchange                  |
| `payments.dlq`            | queue          | Сообщения после 3 неуспешных попыток  |

## Локальная разработка

```bash
python -m venv .venv
source .venv/bin/activate     # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# Поднять только инфраструктуру:
docker compose up postgres rabbitmq -d

# Миграции:
alembic upgrade head

# В трёх отдельных терминалах:
uvicorn app.main:app
python -m app.outbox_publisher
faststream run app.consumer:app
```

