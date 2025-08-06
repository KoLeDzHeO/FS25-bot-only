from __future__ import annotations

import discord
from discord import app_commands

from utils.logger import log_debug
from pause_guard import pause_guard


async def _clear_messages(interaction: discord.Interaction) -> None:
    """Delete all messages sent by this bot in the current channel."""
    await interaction.response.defer(ephemeral=True)

    channel = interaction.channel
    bot_user = interaction.client.user
    if channel is None or bot_user is None:
        await interaction.followup.send("Не удалось получить канал или бота.", ephemeral=True)
        return

    deleted = 0
    try:
        async for message in channel.history(limit=None):
            if message.author.id == bot_user.id:
                try:
                    await message.delete()
                    deleted += 1
                except discord.Forbidden:
                    await interaction.followup.send(
                        "Нет прав на удаление сообщений.", ephemeral=True
                    )
                    return
                except discord.HTTPException as e:
                    log_debug(f"[CMD] clear_bot_messages delete error: {e}")
    except discord.Forbidden:
        await interaction.followup.send(
            "Нет прав на чтение истории сообщений.", ephemeral=True
        )
        return
    except Exception as e:  # pragma: no cover - unexpected errors
        log_debug(f"[CMD] clear_bot_messages error: {e}")
        await interaction.followup.send(
            "Произошла ошибка при удалении сообщений.", ephemeral=True
        )
        return

    if deleted == 0:
        await interaction.followup.send(
            "Нет сообщений для удаления", ephemeral=True
        )
    else:
        await interaction.followup.send(
            f"Удалено сообщений: {deleted}", ephemeral=True
        )


def setup(tree: app_commands.CommandTree) -> None:
    @tree.command(
        name="clear_bot_messages",
        description="Удаляет сообщения, отправленные ботом в текущем канале",
    )
    @pause_guard
    async def clear_bot_messages(interaction: discord.Interaction) -> None:
        await _clear_messages(interaction)

    log_debug("[Slash] Команда /clear_bot_messages зарегистрирована")
