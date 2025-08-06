from __future__ import annotations

import discord
from discord import app_commands

from asyncpg import Pool

from config.config import WEEKLY_TOP_LIMIT, WEEKLY_TOP_LAST_TABLE
from utils.logger import log_debug
from pause_guard import pause_guard


async def _fetch_last_week_top(
    pool: Pool,
    *,
    table_name: str = WEEKLY_TOP_LAST_TABLE,
    limit: int = WEEKLY_TOP_LIMIT,
) -> list[tuple[str, int]]:
    """Fetch archived weekly top rows."""
    try:
        rows = await pool.fetch(
            f"SELECT player_name, hours FROM {table_name} "
            "ORDER BY hours DESC, player_name LIMIT $1",
            limit,
        )
    except Exception as e:
        log_debug(f"[DB] Error fetching last week top: {e}")
        raise

    return [(r["player_name"], int(r["hours"])) for r in rows]


async def _handle_command(
    interaction: discord.Interaction,
    *,
    table_name: str = WEEKLY_TOP_LAST_TABLE,
    limit: int = WEEKLY_TOP_LIMIT,
) -> None:
    await interaction.response.defer()
    pool: Pool = interaction.client.db_pool
    try:
        rows = await _fetch_last_week_top(pool, table_name=table_name, limit=limit)
    except Exception:
        await interaction.followup.send("Ошибка при получении топа.", ephemeral=True)
        return

    if not rows:
        await interaction.followup.send("Нет данных за прошлую неделю.")
        return

    count = min(limit, len(rows))
    lines = [f"\U0001f553 Топ {count} игроков за прошлую неделю:"]
    for idx, (name, hours) in enumerate(rows[:count], start=1):
        lines.append(f"{idx}. {name} — {hours} ч")

    await interaction.followup.send("\n".join(lines))


def setup(
    tree: app_commands.CommandTree,
    *,
    table_name: str = WEEKLY_TOP_LAST_TABLE,
    limit: int = WEEKLY_TOP_LIMIT,
) -> None:
    @tree.command(name="top7lastweek", description="Топ игроков прошлой недели")
    @pause_guard
    async def top7lastweek_command(interaction: discord.Interaction) -> None:
        await _handle_command(interaction, table_name=table_name, limit=limit)

    log_debug("[Slash] Команда /top7lastweek зарегистрирована")
