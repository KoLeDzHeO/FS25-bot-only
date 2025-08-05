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

from utils.logger import log_debug
from commands.top7lastweek import setup as setup_top7lastweek
from commands.top7week import setup as setup_top7week
from commands.top_total import setup as setup_top_total
from commands.online_month import setup as setup_online_month
from commands.export_excel import setup as setup_export_excel
from commands.info import setup as setup_info


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
        self.db_pool = await asyncpg.create_pool(dsn=config.postgres_url)
        await self._ensure_indexes()

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
        log_debug("[SETUP] Background tasks started")

        setup_top7week(self.tree)

        setup_top7lastweek(self.tree)

        setup_top_total(self.tree)
        setup_online_month(self.tree)
        setup_info(self.tree)
        await setup_export_excel(self.tree)
        await self.tree.sync()
        log_debug("[SYNC] Slash-команды успешно синхронизированы")
        log_debug("[Slash] Команды синхронизированы")

    async def on_ready(self) -> None:
        """Log successful authorization."""
        log_debug(f"Discord-бот авторизован как {self.user}")

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

    log_debug("Запускаем Discord-бота")
    try:
        bot.run(config.discord_token)
    except KeyboardInterrupt:
        log_debug("[MAIN] Прерывание, останавливаем бота")
        asyncio.run(bot.close())
    finally:
        log_debug("Discord-бот остановлен")
