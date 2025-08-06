"""Entry point for launching the Discord bot."""

import asyncio

import asyncpg
import discord
from discord import app_commands

from config.config import config
from bot.updater import (
    ftp_polling_task,
    save_online_history_task,
    cleanup_old_online_history_task,
)
from utils.total_time_updater import total_time_update_task
from utils.weekly_archiver import weekly_top_archive_task
from bot.discord_ui import build_paused_embed

from utils.logger import log_debug, log_info
from commands.top7lastweek import setup as setup_top7lastweek
from commands.top7week import setup as setup_top7week
from commands.top_total import setup as setup_top_total
from commands.online_month import setup as setup_online_month
from commands.export_excel import setup as setup_export_excel
from commands.info import setup as setup_info
from commands.clear_bot_messages import setup as setup_clear_bot_messages


def handle_task_exception(task: asyncio.Task) -> None:
    """Log exceptions from background tasks."""
    try:
        task.result()
    except Exception as e:
        log_debug(f"[ERROR] Задача завершилась с ошибкой: {e}")


class MyBot(discord.Client):
    """Discord bot client with background tasks."""

    def __init__(self, *, intents: discord.Intents) -> None:
        super().__init__(intents=intents)
        self.tree = app_commands.CommandTree(self)
        self.tasks: list[asyncio.Task] = []
        self.db_pool = None

    async def _ensure_indexes(self) -> None:
        """Create required database indexes if they do not exist."""
        await self.db_pool.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_online_name_date_hour
            ON player_online_history (player_name, date, hour)
            """
        )
        await self.db_pool.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_online_check_time
            ON player_online_history (check_time)
            """
        )
        await self.db_pool.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_total_hours
            ON player_total_time (total_hours DESC)
            """
        )

    async def close(self) -> None:
        """Gracefully shutdown background tasks and resources."""
        for task in self.tasks:
            task.cancel()
        if self.tasks:
            await asyncio.gather(*self.tasks, return_exceptions=True)
        if self.db_pool:
            await self.db_pool.close()
        await super().close()

    async def setup_hook(self) -> None:
        """Called by discord.py when the client is ready."""
        log_info("[SETUP] Starting bot setup")
        self.db_pool = await asyncpg.create_pool(dsn=config.postgres_url)
        await self._ensure_indexes()
        if config.bot_paused_mode:
            log_info("[SETUP] BOT_PAUSED_MODE enabled - skipping background tasks")
            channel = await self.fetch_channel(config.channel_id)
            if channel is None:
                log_debug("❌ Канал не найден!")
            else:
                bot_user = self.user
                if bot_user and hasattr(channel, "history"):
                    deleted = 0
                    try:
                        async for message in channel.history(limit=None):
                            if message.author.id == bot_user.id:
                                try:
                                    await message.delete()
                                    deleted += 1
                                except discord.Forbidden:
                                    log_debug("[PAUSED] Нет прав на удаление сообщений")
                                    break
                                except discord.HTTPException as e:
                                    log_debug(f"[PAUSED] Ошибка удаления сообщения: {e}")
                        log_debug(f"[PAUSED] Удалено сообщений: {deleted}")
                    except discord.Forbidden:
                        log_debug("[PAUSED] Нет прав на чтение истории сообщений")
                    except Exception as e:
                        log_debug(f"[PAUSED] Неожиданная ошибка при удалении сообщений: {e}")
                else:
                    log_debug("[PAUSED] Канал не поддерживает историю сообщений")
                embed = build_paused_embed()
                await channel.send(embed=embed)
                log_info("[PAUSED] Отправлено сообщение о недоступности сервера")
        else:
            log_info("[SETUP] Starting background tasks")
            task = asyncio.create_task(ftp_polling_task(self))
            task.add_done_callback(handle_task_exception)
            self.tasks.append(task)

            task = asyncio.create_task(save_online_history_task(self))
            task.add_done_callback(handle_task_exception)
            self.tasks.append(task)

            task = asyncio.create_task(cleanup_old_online_history_task(self))
            task.add_done_callback(handle_task_exception)
            self.tasks.append(task)

            task = asyncio.create_task(total_time_update_task(self))
            task.add_done_callback(handle_task_exception)
            self.tasks.append(task)

            task = asyncio.create_task(weekly_top_archive_task(self))
            task.add_done_callback(handle_task_exception)
            self.tasks.append(task)
            log_info("[SETUP] Background tasks started")

        setup_top7week(self.tree)

        setup_top7lastweek(self.tree)

        setup_top_total(self.tree)
        setup_online_month(self.tree)
        setup_info(self.tree)
        setup_clear_bot_messages(self.tree)
        await setup_export_excel(self.tree)
        await self.tree.sync()
        log_debug("[SYNC] Slash-команды успешно синхронизированы")
        log_debug("[Slash] Команды синхронизированы")

    async def on_ready(self) -> None:
        """Log successful authorization."""
        log_info(f"Discord-бот авторизован как {self.user}")

    async def on_message(self, message: discord.Message) -> None:
        """Обрабатывает текстовые сообщения (команды больше не используются)."""
        if message.author.bot:
            return


if __name__ == "__main__":
    intents = discord.Intents.default()
    intents.messages = True
    intents.guilds = True
    intents.message_content = True

    bot = MyBot(intents=intents)

    log_info("Запускаем Discord-бота")
    try:
        bot.run(config.discord_token)
    except KeyboardInterrupt:
        log_info("[MAIN] Прерывание, останавливаем бота")
        asyncio.run(bot.close())
    finally:
        log_info("Discord-бот остановлен")
