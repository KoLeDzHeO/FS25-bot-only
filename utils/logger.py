"""Простейший логгер для вывода сообщений."""

import logging
import os

log_level = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, log_level, logging.INFO), format="%(message)s"
)


def log_debug(message: str) -> None:
    """Выводит отладочное сообщение."""
    logging.info(message)
