"""Background tasks for updating and storing server information."""

import asyncio
from datetime import datetime, timedelta

import aiohttp
import discord

from config.config import (
    ONLINE_DAILY_GRAPH_FILENAME,
    cleanup_history_days,
    cleanup_task_interval_seconds,
    config,
)
from utils.logger import log_debug
from utils.online_daily_graph import fetch_daily_online_counts, save_daily_online_graph

from .discord_ui import build_embed
from .fetchers import fetch_dedicated_server_stats, fetch_required_files
from .parsers import parse_all, parse_players_online


async def ftp_polling_task(bot: discord.Client, session: aiohttp.ClientSession) -> None:
    """Periodically updates the Discord message with server stats."""
    log_debug("[TASK] Запущен ftp_polling_task")
    await bot.wait_until_ready()
    channel = await bot.fetch_channel(config.channel_id)
    if channel is None:
        log_debug("❌ Канал не найден!")
        return

    while not bot.is_closed():
        try:
            (
                stats_xml,
                vehicles_xml,
                career_ftp,
                farmland_ftp,
                farms_ftp,
            ) = await fetch_required_files(session)
            dedicated_server_stats_ftp = stats_xml

            log_debug(
                "[DEBUG] Статусы: "
                f"stats={bool(stats_xml)}, "
                f"vehicles={bool(vehicles_xml)}, "
                f"careerFTP={bool(career_ftp)}, "
                f"farmlandFTP={bool(farmland_ftp)}, "
                f"farms={bool(farms_ftp)}"
            )

            all_files_loaded = all(
                [stats_xml, vehicles_xml, career_ftp, farmland_ftp, farms_ftp]
            )
            if all_files_loaded:
                server_status = "🟢 Сервер работает"
                log_debug("[FTP] Все необходимые файлы загружены")
                data = parse_all(
                    server_stats=stats_xml,
                    vehicles_api=vehicles_xml,
                    career_savegame_ftp=career_ftp,
                    farmland_ftp=farmland_ftp,
                    farms_xml=farms_ftp,
                    dedicated_server_stats=dedicated_server_stats_ftp,
                )
            else:
                server_status = "🔴 Сервер недоступен"
                data = {
                    "last_month_profit": None,
                    "server_name": None,
                    "map_name": None,
                    "slots_used": None,
                    "slots_max": None,
                    "farm_money": None,
                    "fields_owned": None,
                    "fields_total": None,
                    "vehicles_owned": None,
                    "players_online": [],
                }

            data["server_status"] = server_status
            embed = build_embed(data)

            hourly_counts = await fetch_daily_online_counts(bot.db_pool)

            image_path = save_daily_online_graph(hourly_counts)
            embed.set_image(url=f"attachment://{ONLINE_DAILY_GRAPH_FILENAME}")

            async for msg in channel.history(limit=20):
                if msg.author == bot.user:
                    try:
                        await msg.delete()
                    except Exception as e:
                        log_debug(f"[Discord] Не удалось удалить сообщение: {e}")

            log_debug("[Discord] Отправляем сообщение")
            await channel.send(
                embed=embed,
                files=[discord.File(image_path, filename=ONLINE_DAILY_GRAPH_FILENAME)],
            )
            log_debug("[Discord] ✅ Embed успешно отправлен.")

            await asyncio.sleep(config.ftp_poll_interval)
        except asyncio.CancelledError:
            log_debug("[TASK] ftp_polling_task cancelled")
            break
        except Exception as e:
            log_debug(f"[TASK] ftp_polling_task error: {e}")
            await asyncio.sleep(5)


async def api_polling_task(session: aiohttp.ClientSession) -> None:
    """Периодически опрашивает API и сохраняет онлайн игроков."""
    while True:
        try:
            await fetch_dedicated_server_stats(session)
            await asyncio.sleep(config.api_poll_interval)
        except asyncio.CancelledError:
            log_debug("[TASK] api_polling_task cancelled")
            break
        except Exception as e:
            log_debug(f"[TASK] api_polling_task error: {e}")
            await asyncio.sleep(5)


async def save_online_history_task(
    bot: discord.Client, session: aiohttp.ClientSession
) -> None:
    """Сохраняет список онлайн-игроков каждые 15 минут."""
    log_debug("[TASK] Запущен save_online_history_task")
    await bot.wait_until_ready()
    while not bot.is_closed():
        try:
            now = datetime.now()
            minute = now.minute

            now_utc = datetime.utcnow().replace(tzinfo=None)
            now_moscow = now_utc + timedelta(hours=config.timezone_offset)

            if minute % 15 == 0:
                log_debug("[ONLINE] Время среза, получаем список игроков")
                xml = await fetch_dedicated_server_stats(session)
                players = parse_players_online(xml) if xml else []
                log_debug(f"[ONLINE] Игроков онлайн: {len(players)}")
                records = [(name, now_moscow) for name in players]
                if records:
                    try:
                        await bot.db_pool.executemany(
                            """
                            INSERT INTO player_online_history (
                                player_name, check_time, date, hour, dow
                            ) VALUES (
                                $1, $2, DATE($2), EXTRACT(HOUR FROM $2), EXTRACT(DOW FROM $2)
                            )
                            ON CONFLICT (player_name, date, hour) DO NOTHING
                            """,
                            records,
                        )
                    except Exception as db_e:
                        log_debug(f"[DB] Ошибка записи игрока: {db_e}")

                wait_seconds = 60
            else:
                wait_seconds = ((15 - (minute % 15)) * 60) - now.second
                if wait_seconds <= 0:
                    wait_seconds = 1
                log_debug(f"[ONLINE] Не время среза, ждём {wait_seconds} секунд")
            await asyncio.sleep(wait_seconds)
        except asyncio.CancelledError:
            log_debug("[TASK] save_online_history_task cancelled")
            break
        except Exception as e:
            log_debug(f"[TASK] save_online_history_task error: {e}")
            await asyncio.sleep(5)


async def cleanup_old_online_history_task(bot: discord.Client) -> None:
    """Удаляет записи старше 30 дней из player_online_history."""
    log_debug("[TASK] Запущен cleanup_old_online_history_task")
    await bot.wait_until_ready()
    while not bot.is_closed():
        try:
            log_debug("[DB] Удаляем старые записи из player_online_history")
            await bot.db_pool.execute(
                f"DELETE FROM player_online_history WHERE check_time < NOW() - INTERVAL '{cleanup_history_days} days'"
            )
            await asyncio.sleep(cleanup_task_interval_seconds)
        except asyncio.CancelledError:
            log_debug("[TASK] cleanup_old_online_history_task cancelled")
            break
        except Exception as e:
            log_debug(f"[TASK] cleanup_old_online_history_task error: {e}")
            await asyncio.sleep(5)
