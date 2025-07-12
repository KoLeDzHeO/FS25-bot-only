"""Utilities for updating total play time of players."""

from __future__ import annotations

import asyncio
from typing import List, Tuple

from asyncpg import Pool

from config.config import cleanup_history_days
from utils.logger import log_debug


async def _fetch_total_hours(
    db_pool: Pool,
    *,
    history_table: str = "player_online_history",
) -> List[Tuple[str, int]]:
    """Return total active hours for each player from history."""
    # Собираем уникальные часовые интервалы с тремя и более отметками
    query = f"""
        SELECT player_name, COUNT(*) AS hours
        FROM (
            SELECT player_name, date, hour
            FROM {history_table}
            WHERE check_time >= NOW() - INTERVAL '{cleanup_history_days} days'
            GROUP BY player_name, date, hour
            HAVING COUNT(*) >= 3
        ) AS t
        GROUP BY player_name
        ORDER BY player_name
    """
    try:
        rows = await db_pool.fetch(query)
    except Exception as e:
        log_debug(f"[DB] Error fetching total hours: {e}")
        raise

    return [(r["player_name"], int(r["hours"])) for r in rows]


async def update_total_time(
    db_pool: Pool,
    *,
    history_table: str = "player_online_history",
    total_table: str = "player_total_time",
) -> None:
    """Calculate total hours and update the total time table."""
    rows = await _fetch_total_hours(db_pool, history_table=history_table)

    if not rows:
        log_debug("[TOTAL] Нет данных для обновления")
        return

    try:
        async with db_pool.acquire() as conn:
            async with conn.transaction():
                # UPSERT обновляет время, если игрок уже есть в таблице
                await conn.executemany(
                    f"""
                    INSERT INTO {total_table} (player_name, total_hours, updated_at)
                    VALUES ($1, $2, NOW())
                    ON CONFLICT (player_name) DO UPDATE
                        SET total_hours = EXCLUDED.total_hours,
                            updated_at = EXCLUDED.updated_at
                    """,
                    rows,
                )
        log_debug(f"[TOTAL] Обновлено {len(rows)} записей")
    except Exception as e:
        log_debug(f"[DB] Error updating total time: {e}")
        raise


async def total_time_update_task(
    bot,
    *,
    interval_seconds: int = 3600,
    history_table: str = "player_online_history",
    total_table: str = "player_total_time",
) -> None:
    """Background task to periodically update player total time."""
    log_debug("[TASK] Запущен total_time_update_task")
    await bot.wait_until_ready()
    while not bot.is_closed():
        try:
            await update_total_time(
                bot.db_pool,
                history_table=history_table,
                total_table=total_table,
            )
            await asyncio.sleep(interval_seconds)
        except asyncio.CancelledError:
            log_debug("[TASK] total_time_update_task cancelled")
            break
        except Exception as e:
            log_debug(f"[TASK] total_time_update_task error: {e}")
            await asyncio.sleep(5)
