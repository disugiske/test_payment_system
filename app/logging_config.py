import logging
import sys

from pythonjsonlogger import jsonlogger

from app.config import get_settings


def setup_logging() -> None:
    settings = get_settings()
    handler = logging.StreamHandler(sys.stdout)
    formatter = jsonlogger.JsonFormatter(
        "%(asctime)s %(levelname)s %(name)s %(message)s"
    )
    handler.setFormatter(formatter)

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(settings.log_level)

    for noisy in ("aio_pika", "aiormq"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
