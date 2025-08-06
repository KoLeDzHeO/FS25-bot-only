from __future__ import annotations

"""Decorator to block slash commands when the bot is paused."""

from functools import wraps
from typing import Any, Awaitable, Callable, TypeVar, cast

import discord

from config.config import config

PAUSE_MESSAGE = "\U0001f6d1 Сервер находится в режиме паузы. Команды временно недоступны."

F = TypeVar("F", bound=Callable[..., Awaitable[None]])


def pause_guard(func: F) -> F:
    """Prevent command execution if bot is paused."""

    @wraps(func)
    async def wrapper(*args: Any, **kwargs: Any) -> None:
        interaction: discord.Interaction | None = None
        if args:
            interaction = cast(discord.Interaction, args[0])
        elif "interaction" in kwargs:
            interaction = cast(discord.Interaction, kwargs["interaction"])
        if config.bot_paused_mode and interaction is not None:
            if interaction.response.is_done():
                await interaction.followup.send(PAUSE_MESSAGE, ephemeral=True)
            else:
                await interaction.response.send_message(PAUSE_MESSAGE, ephemeral=True)
            return
        await func(*args, **kwargs)

    return cast(F, wrapper)
