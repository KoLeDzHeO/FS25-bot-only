"""Логика для подсчёта недельного топа игроков."""

from datetime import datetime, timedelta
from typing import List

from config.config import (
    WEEKLY_TOP_LIMIT,
    WEEKLY_TOP_MAX,
    WEEKLY_TOP_WEEKDAY,
    WEEKLY_TOP_HOUR,
)
from utils.helpers import get_moscow_datetime
from utils.logger import log_debug


def _get_week_bounds() -> tuple[datetime, datetime]:
    """Возвращает начало и конец недельного периода."""
    now = get_moscow_datetime()
    start = now.replace(
        hour=WEEKLY_TOP_HOUR,
        minute=0,
        second=0,
        microsecond=0,
    )
    days_since = (now.weekday() - WEEKLY_TOP_WEEKDAY) % 7
    start -= timedelta(days=days_since)
    if now < start:
        start -= timedelta(days=7)
    end = start + timedelta(days=7)
    return start, end


async def generate_weekly_top(db_pool) -> str:
    """Формирует текстовое сообщение с топом игроков за неделю."""
    start, end = _get_week_bounds()
    log_debug(f"[TOP] Период с {start} по {end}")
    try:
        rows = await db_pool.fetch(
            """
            SELECT player_name, COUNT(*) AS hours
            FROM (
                SELECT player_name, date, hour
                FROM player_online_history
                WHERE check_time >= $1 AND check_time < $2
                GROUP BY player_name, date, hour
                HAVING COUNT(*) >= 3
            ) AS t
            GROUP BY player_name
            ORDER BY hours DESC, player_name
            LIMIT $3
            """,
            start,
            end,
            WEEKLY_TOP_MAX,
        )
    except Exception as e:
        log_debug(f"[DB] Error fetching weekly top: {e}")
        raise

    if not rows:
        return "Нет данных за неделю."

    limit = min(WEEKLY_TOP_LIMIT, len(rows))
    lines: List[str] = [f"\U0001f4ca ТОП {limit} игроков за неделю:"]
    for idx, row in enumerate(rows[:limit], start=1):
        lines.append(f"{idx}. {row['player_name']} — {row['hours']} ч")

    return "\n".join(lines)
