"""Utilities for updating total play time of players."""

from __future__ import annotations

import asyncio

from asyncpg import Pool

from utils.logger import log_debug
from config.config import config


async def update_total_time(
    db_pool: Pool,
    *,
    history_table: str = "player_online_history",
    total_table: str = "player_total_time",
) -> None:
    """Добавляет игрокам только новые часы из истории."""

    try:
        async with db_pool.acquire() as conn:
            async with conn.transaction():
                # Создаём таблицу, если её ещё нет
                await conn.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {total_table} (
                        id SERIAL PRIMARY KEY,
                        player_name TEXT UNIQUE NOT NULL,
                        total_hours INTEGER NOT NULL DEFAULT 0,
                        last_processed_at TIMESTAMP NOT NULL DEFAULT '2000-01-01 00:00:00',
                        updated_at TIMESTAMP NOT NULL
                    )
                    """
                )

                # Убедимся, что все игроки присутствуют в таблице total_table
                await conn.execute(
                    f"""
                    INSERT INTO {total_table} (player_name, total_hours, last_processed_at, updated_at)
                    SELECT DISTINCT player_name, 0, NOW(), NOW()
                    FROM {history_table}
                    ON CONFLICT (player_name) DO NOTHING
                    """
                )

                # Инициализируем игроков без учтённых часов
                init_rows = await conn.fetch(
                    f"""
                    WITH max_ts AS (
                        SELECT player_name,
                               MAX(date::timestamp + hour * INTERVAL '1 hour') AS ts
                        FROM {history_table}
                        GROUP BY player_name
                    )
                    UPDATE {total_table} t
                    SET last_processed_at = m.ts,
                        updated_at = NOW()
                    FROM max_ts m
                    WHERE t.player_name = m.player_name
                      AND t.last_processed_at = '2000-01-01 00:00:00'
                      AND m.ts IS NOT NULL
                    RETURNING t.player_name
                    """
                )
                if init_rows:
                    log_debug(f"[TOTAL] Инициализировано {len(init_rows)} игроков")

                # Добавляем только новые часы
                updated_rows = await conn.fetch(
                    f"""
                    WITH hourly AS (
                        SELECT player_name,
                               date::timestamp + hour * INTERVAL '1 hour' AS ts
                        FROM {history_table}
                        GROUP BY player_name, date, hour
                        HAVING COUNT(*) >= 3
                    ),
                    new_hours AS (
                        SELECT h.player_name,
                               COUNT(*) AS hours,
                               MAX(h.ts) AS max_ts
                        FROM hourly h
                        JOIN {total_table} t ON t.player_name = h.player_name
                        WHERE h.ts > t.last_processed_at
                          AND t.last_processed_at > '2000-01-01 00:00:00'
                        GROUP BY h.player_name
                    )
                    UPDATE {total_table} t
                    SET total_hours = t.total_hours + n.hours,
                        last_processed_at = n.max_ts,
                        updated_at = NOW()
                    FROM new_hours n
                    WHERE t.player_name = n.player_name
                    RETURNING t.player_name
                    """
                )
                if updated_rows:
                    log_debug(f"[TOTAL] Обновлено {len(updated_rows)} записей")
                else:
                    log_debug("[TOTAL] Нет данных для обновления")

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
