from __future__ import annotations

import os

import discord
from discord import app_commands

from config.config import ONLINE_MONTH_GRAPH_TITLE
from utils.logger import log_debug
from utils.online_month_graph import generate_online_month_graph


def setup(tree: app_commands.CommandTree) -> None:
    @tree.command(
        name="online_month",
        description="График онлайна по дням за последние 30 дней",
    )
    async def online_month_command(interaction: discord.Interaction) -> None:
        await interaction.response.defer()
        try:
            path = await generate_online_month_graph(interaction.client.db_pool)
            if not path:
                await interaction.followup.send("Нет данных за последний месяц.")
                return
            embed = discord.Embed(title=ONLINE_MONTH_GRAPH_TITLE)
            embed.set_image(url=f"attachment://{os.path.basename(path)}")
            await interaction.followup.send(
                embed=embed,
                file=discord.File(path, filename=os.path.basename(path)),
            )
        except Exception as e:
            log_debug(f"[CMD] online_month error: {e}")
            await interaction.followup.send(
                "Ошибка при генерации графика.", ephemeral=True
            )

    log_debug("[Slash] Команда /online_month зарегистрирована")
