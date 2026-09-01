"""Webhook delivery semantics: retry on transient failures, not on 4xx."""
from __future__ import annotations

from typing import Any

import httpx
import pytest
from pytest_httpx import HTTPXMock

from app.webhook import WebhookDeliveryError, send_webhook

pytestmark = pytest.mark.unit

URL = "https://example.com/hook"
PAYLOAD: dict[str, Any] = {"payment_id": "x", "status": "succeeded"}


@pytest.fixture(autouse=True)
def _fast_retries(monkeypatch: pytest.MonkeyPatch) -> None:
    """Disable tenacity backoff sleeps so retry tests stay snappy."""
    from app import webhook
    monkeypatch.setattr(webhook, "wait_exponential", lambda **_: lambda *_a, **_k: 0)


async def test_2xx_no_retry(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(url=URL, method="POST", status_code=200)
    await send_webhook(URL, PAYLOAD)
    assert len(httpx_mock.get_requests()) == 1


async def test_4xx_does_not_retry(httpx_mock: HTTPXMock) -> None:
    """Client mistakes are not retried — wastes time and confuses the receiver."""
    httpx_mock.add_response(url=URL, method="POST", status_code=400)
    await send_webhook(URL, PAYLOAD)  # must not raise
    assert len(httpx_mock.get_requests()) == 1


async def test_5xx_retried_then_raises(
    httpx_mock: HTTPXMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    from app import webhook
    monkeypatch.setattr(webhook.settings, "webhook_max_attempts", 3)

    for _ in range(3):
        httpx_mock.add_response(url=URL, method="POST", status_code=503)

    with pytest.raises(WebhookDeliveryError):
        await send_webhook(URL, PAYLOAD)
    assert len(httpx_mock.get_requests()) == 3


async def test_network_error_retried_then_raises(
    httpx_mock: HTTPXMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    from app import webhook
    monkeypatch.setattr(webhook.settings, "webhook_max_attempts", 2)

    for _ in range(2):
        httpx_mock.add_exception(httpx.ConnectError("boom"), url=URL, method="POST")

    with pytest.raises(WebhookDeliveryError):
        await send_webhook(URL, PAYLOAD)
    assert len(httpx_mock.get_requests()) == 2


async def test_success_after_transient_failure(
    httpx_mock: HTTPXMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    from app import webhook
    monkeypatch.setattr(webhook.settings, "webhook_max_attempts", 3)

    httpx_mock.add_response(url=URL, method="POST", status_code=503)
    httpx_mock.add_response(url=URL, method="POST", status_code=200)

    await send_webhook(URL, PAYLOAD)
    assert len(httpx_mock.get_requests()) == 2
