"""Вспомогательные функции для работы со временем."""

from datetime import datetime, timedelta

from config.config import config


def get_moscow_time() -> str:
    """Возвращает текущее локальное время в формате YYYY-MM-DD HH:MM:SS."""
    return (datetime.utcnow() + timedelta(hours=config.timezone_offset)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )


def get_moscow_datetime() -> datetime:
    """Возвращает объект datetime с учётом смещения часового пояса."""
    return datetime.utcnow() + timedelta(hours=config.timezone_offset)
