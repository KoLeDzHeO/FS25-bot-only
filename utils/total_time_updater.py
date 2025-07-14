"""Utilities for updating total play time of players."""

from __future__ import annotations

import asyncio
from typing import List, Tuple

from asyncpg import Pool

from utils.logger import log_debug
from config.config import config


async def _fetch_incremental_hours(
    db_pool: Pool,
    *,
    history_table: str = "player_online_history",
    total_table: str = "player_total_time",
) -> List[Tuple[str, int]]:
    """Return active hours for each player since the stored timestamp."""
    query = f"""
        SELECT player_name, COUNT(*) AS hours
        FROM (
            SELECT h.player_name, h.date, h.hour
            FROM {history_table} AS h
            LEFT JOIN {total_table} AS t ON h.player_name = t.player_name
            WHERE h.check_time > COALESCE(t.last_timestamp, TIMESTAMP 'epoch')
            GROUP BY h.player_name, h.date, h.hour
            HAVING COUNT(*) >= 3
        ) AS s
        GROUP BY player_name
        ORDER BY player_name
    """
    try:
        rows = await db_pool.fetch(query)
    except Exception as e:
        log_debug(f"[DB] Error fetching incremental hours: {e}")
        raise

    return [(r["player_name"], int(r["hours"])) for r in rows]


async def update_total_time(
    db_pool: Pool,
    *,
    history_table: str = "player_online_history",
    total_table: str = "player_total_time",
) -> None:
    """Calculate incremental hours and update the total time table."""

    try:
        async with db_pool.acquire() as conn:
            async with conn.transaction():
                # Создаём таблицу, если её ещё нет (нужен PRIMARY KEY для ON CONFLICT)
                await conn.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {total_table} (
                        player_name TEXT PRIMARY KEY,
                        total_hours INTEGER NOT NULL,
                        updated_at TIMESTAMP NOT NULL,
                        last_timestamp TIMESTAMP NOT NULL DEFAULT 'epoch'
                    )
                    """
                )
                await conn.execute(
                    f"ALTER TABLE {total_table} "
                    "ADD COLUMN IF NOT EXISTS last_timestamp TIMESTAMP NOT NULL DEFAULT 'epoch'"
                )

                rows = await _fetch_incremental_hours(
                    conn,
                    history_table=history_table,
                    total_table=total_table,
                )

                if rows:
                    await conn.executemany(
                        f"""
                        INSERT INTO {total_table} (player_name, total_hours, updated_at, last_timestamp)
                        VALUES ($1, $2, NOW(), NOW())
                        ON CONFLICT (player_name) DO UPDATE
                            SET total_hours = {total_table}.total_hours + EXCLUDED.total_hours,
                                updated_at = EXCLUDED.updated_at,
                                last_timestamp = EXCLUDED.last_timestamp
                        """,
                        rows,
                    )
                    log_debug(f"[TOTAL] Обновлено {len(rows)} записей")
                else:
                    log_debug("[TOTAL] Нет данных для обновления")

                await conn.execute(f"UPDATE {total_table} SET last_timestamp = NOW()")
    except Exception as e:
        log_debug(f"[DB] Error updating total time: {e}")
        raise


async def total_time_update_task(
    bot,
    *,
    interval_seconds: int = config.total_time_interval,
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
