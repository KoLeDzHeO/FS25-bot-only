"""Archive weekly top players to database."""

from __future__ import annotations

import asyncio
from datetime import timedelta, datetime
from typing import List, Tuple

from asyncpg import Pool

from config.config import (
    WEEKLY_TOP_LIMIT,
    WEEKLY_TOP_MAX,
    WEEKLY_TOP_WEEKDAY,
    WEEKLY_TOP_HOUR,
)
from utils.weekly_top import _get_week_bounds
from utils.helpers import get_moscow_datetime
from utils.logger import log_debug


async def _fetch_top_rows(
    db_pool: Pool, start: datetime, end: datetime, limit: int
) -> List[Tuple[str, int]]:
    """Return weekly top rows from history."""
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
            limit,
        )
    except Exception as e:
        log_debug(f"[DB] Error fetching weekly top: {e}")
        raise

    return [(r["player_name"], int(r["hours"])) for r in rows]


async def archive_weekly_top(
    db_pool: Pool,
    *,
    table_name: str = "weekly_top_last",
    limit: int = WEEKLY_TOP_LIMIT,
    max_fetch: int = WEEKLY_TOP_MAX,
) -> None:
    """Calculate last week's top players and store them in the table."""
    start, end = _get_week_bounds()
    start -= timedelta(days=7)
    end -= timedelta(days=7)
    log_debug(f"[ARCHIVER] Период с {start} по {end}")

    rows = await _fetch_top_rows(db_pool, start, end, max_fetch)
    rows = rows[:limit]

    if not rows:
        log_debug("[ARCHIVER] Нет данных для записи")
        return

    try:
        async with db_pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        player_name TEXT PRIMARY KEY,
                        hours INTEGER NOT NULL
                    )
                    """
                )
                await conn.execute(f"TRUNCATE TABLE {table_name}")
                await conn.executemany(
                    f"""
                    INSERT INTO {table_name} (player_name, hours)
                    VALUES ($1, $2)
                    ON CONFLICT (player_name) DO UPDATE
                    SET hours = EXCLUDED.hours
                    """,
                    rows,
                )
        log_debug("[ARCHIVER] Топ игроков сохранён")
    except Exception as e:
        log_debug(f"[DB] Error writing weekly top: {e}")
        raise


def _seconds_until_next_run(weekday: int, hour: int) -> float:
    now = get_moscow_datetime()
    next_run = now.replace(hour=hour, minute=0, second=0, microsecond=0)
    days = (weekday - now.weekday()) % 7
    next_run += timedelta(days=days)
    if next_run <= now:
        next_run += timedelta(days=7)
    return (next_run - now).total_seconds()


async def weekly_top_archive_task(
    bot,
    *,
    table_name: str = "weekly_top_last",
    weekday: int = WEEKLY_TOP_WEEKDAY,
    hour: int = WEEKLY_TOP_HOUR,
    limit: int = WEEKLY_TOP_LIMIT,
    max_fetch: int = WEEKLY_TOP_MAX,
) -> None:
    """Background task to archive weekly top players."""
    log_debug("[TASK] Запущен weekly_top_archive_task")
    await bot.wait_until_ready()
    while not bot.is_closed():
        try:
            wait_seconds = _seconds_until_next_run(weekday, hour)
            log_debug(f"[ARCHIVER] Следующий запуск через {int(wait_seconds)} секунд")
            await asyncio.sleep(wait_seconds)
            await archive_weekly_top(
                bot.db_pool,
                table_name=table_name,
                limit=limit,
                max_fetch=max_fetch,
            )
        except asyncio.CancelledError:
            log_debug("[TASK] weekly_top_archive_task cancelled")
            break
        except Exception as e:
            log_debug(f"[TASK] weekly_top_archive_task error: {e}")
            await asyncio.sleep(5)
