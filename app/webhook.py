import logging
from typing import Any

import httpx
from starlette import status
from tenacity import (
    AsyncRetrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from app.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()


class WebhookDeliveryError(Exception):
    pass


async def send_webhook(url: str, payload: dict[str, Any]) -> None:
    """
    Deliver a webhook with exponential-backoff retries.

    Retries on network errors and on 5xx responses. Raises
    `WebhookDeliveryError` after exhausting attempts.
    """
    async for attempt in AsyncRetrying(
        stop=stop_after_attempt(settings.webhook_max_attempts),
        wait=wait_exponential(multiplier=1, min=1, max=30),
        retry=retry_if_exception_type(WebhookDeliveryError),
        reraise=True,
    ):
        with attempt:
            try:
                async with httpx.AsyncClient(
                    timeout=settings.webhook_timeout
                ) as client:
                    response = await client.post(url, json=payload)
            except httpx.HTTPError as exc:
                logger.warning(
                    "webhook network error",
                    extra={"url": url, "error": str(exc)},
                )
                raise WebhookDeliveryError(str(exc)) from exc

            if response.status_code >= status.HTTP_500_INTERNAL_SERVER_ERROR:
                logger.warning(
                    "webhook server error",
                    extra={"url": url, "status": response.status_code},
                )
                raise WebhookDeliveryError(f"HTTP {response.status_code}")

            if response.status_code >= status.HTTP_400_BAD_REQUEST:
                logger.error(
                    "webhook client error – not retrying",
                    extra={"url": url, "status": response.status_code},
                )
                # 4xx is a client mistake; don't retry.
                return

            logger.info(
                "webhook delivered",
                extra={"url": url, "status": response.status_code},
            )
            return
