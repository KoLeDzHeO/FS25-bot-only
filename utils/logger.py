"""Простейший логгер для вывода сообщений."""

import logging

logging.basicConfig(level=logging.INFO, format="%(message)s")


def log_debug(message: str) -> None:
    """Выводит отладочное сообщение."""
    logging.info(message)


def log_info(message: str) -> None:
    """Выводит информационное сообщение."""
    logging.info(message)
