from __future__ import annotations

import discord
from discord import app_commands

from utils.logger import log_debug
from utils.weekly_top import generate_weekly_top


def setup(tree: app_commands.CommandTree) -> None:
    @tree.command(name="top7week", description="Топ 7 игроков за неделю по часам")
    async def top7week_command(interaction: discord.Interaction) -> None:
        await interaction.response.defer()
        try:
            text = await generate_weekly_top(interaction.client.db_pool)
            await interaction.followup.send(text)
        except Exception as e:
            log_debug(f"[CMD] top7week error: {e}")
            await interaction.followup.send(
                "Ошибка при получении топа.", ephemeral=True
            )

    log_debug("[Slash] Команда /top7week зарегистрирована")
