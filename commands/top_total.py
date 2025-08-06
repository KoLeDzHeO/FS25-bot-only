from __future__ import annotations

import discord
from discord import app_commands

from asyncpg import Pool

from utils.logger import log_debug
from pause_guard import pause_guard
from config.config import TOTAL_TOP_LIMIT, TOTAL_TOP_TABLE


async def _fetch_top_total(
    pool: Pool,
    *,
    table_name: str = TOTAL_TOP_TABLE,
    limit: int = TOTAL_TOP_LIMIT,
) -> tuple[list[tuple[str, int]], int]:
    """Возвращает список игроков и общее количество записей."""
    # Считаем часы игрока и общее число строк через оконную функцию
    try:
        rows = await pool.fetch(
            f"""
            SELECT player_name, total_hours,
                   COUNT(*) OVER () AS total_count
            FROM {table_name}
            ORDER BY total_hours DESC, player_name
            LIMIT $1
            """,
            limit,
        )
    except Exception as e:
        log_debug(f"[DB] Error fetching total top: {e}")
        raise

    if not rows:
        return ([], 0)

    total = int(rows[0]["total_count"]) if rows else 0
    return (
        [(r["player_name"], int(r["total_hours"])) for r in rows],
        total,
    )


async def _handle_command(
    interaction: discord.Interaction,
    *,
    table_name: str = TOTAL_TOP_TABLE,
    limit: int = TOTAL_TOP_LIMIT,
) -> None:
    await interaction.response.defer()
    pool: Pool = interaction.client.db_pool
    try:
        rows, total = await _fetch_top_total(pool, table_name=table_name, limit=limit)
    except Exception:
        await interaction.followup.send("Ошибка при получении топа.", ephemeral=True)
        return

    if not rows:
        await interaction.followup.send("Нет данных.")
        return

    lines = ["\U0001f3c6 Топ игроков по общему времени:"]
    for idx, (name, hours) in enumerate(rows, start=1):
        lines.append(f"{idx}. {name} — {hours} ч")

    if total > limit:
        lines.append(f"Показаны только первые {limit} игроков из {total}.")

    await interaction.followup.send("\n".join(lines))


def setup(
    tree: app_commands.CommandTree,
    *,
    table_name: str = TOTAL_TOP_TABLE,
    limit: int = TOTAL_TOP_LIMIT,
) -> None:
    @tree.command(name="top_total", description="Топ игроков по общему времени")
    @pause_guard
    async def top_total_command(interaction: discord.Interaction) -> None:
        await _handle_command(interaction, table_name=table_name, limit=limit)

    log_debug("[Slash] Команда /top_total зарегистрирована")
