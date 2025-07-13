"""Background tasks for updating and storing server information."""

import asyncio
from datetime import timedelta

from utils.helpers import get_moscow_datetime

import aiohttp
import discord
import json

from config.config import (
    config,
    cleanup_history_days,
    cleanup_task_interval_seconds,
    ONLINE_DAILY_GRAPH_FILENAME,
)
from .fetchers import (
    fetch_dedicated_server_stats_cached,
    fetch_required_files,
)
from .parsers import parse_all, parse_players_online
from .discord_ui import build_embed
from utils.online_daily_graph import (
    save_daily_online_graph,
    fetch_daily_online_counts,
)
from utils.logger import log_debug
import time


async def ftp_polling_task(bot: discord.Client) -> None:
    """Periodically send server stats to Discord every polling interval."""
    log_debug("[TASK] Запущен ftp_polling_task")
    await bot.wait_until_ready()
    channel = await bot.fetch_channel(config.channel_id)
    if channel is None:
        log_debug("❌ Канал не найден!")
        return

    timeout = aiohttp.ClientTimeout(total=config.http_timeout)
    last_snapshot: str | None = None
    last_play_time_check: float = 0.0
    last_play_time_value: float | None = None

    async with aiohttp.ClientSession(timeout=timeout) as session:
        while not bot.is_closed():
            try:
                (
                    stats_xml,
                    vehicles_xml,
                    career_api_xml,
                    career_ftp,
                    farmland_ftp,
                    farms_ftp,
                ) = await fetch_required_files(session)
                dedicated_server_stats_ftp = stats_xml

                log_debug(
                    "[DEBUG] Статусы: "
                    f"stats={bool(stats_xml)}, "
                    f"vehicles={bool(vehicles_xml)}, "
                    f"careerAPI={bool(career_api_xml)}, "
                    f"careerFTP={bool(career_ftp)}, "
                    f"farmlandFTP={bool(farmland_ftp)}, "
                    f"farms={bool(farms_ftp)}"
                )

                all_files_loaded = all(
                    [
                        stats_xml,
                        vehicles_xml,
                        career_api_xml,
                        career_ftp,
                        farmland_ftp,
                        farms_ftp,
                    ]
                )
                if all_files_loaded:
                    server_status = "🟢 Сервер работает"
                    log_debug("[FTP] Все необходимые файлы загружены")
                    data = parse_all(
                        server_stats=stats_xml,
                        vehicles_api=vehicles_xml,
                        career_savegame_ftp=career_ftp,
                        farmland_ftp=farmland_ftp,
                        career_savegame_api=career_api_xml,
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
                        "day_time": None,
                        "time_scale": None,
                        "play_time": None,
                        "players_online": [],
                    }

                data["server_status"] = server_status

                play_time_new = data.get("play_time")
                now = time.monotonic()
                if (
                    play_time_new is not None
                    and last_play_time_value is not None
                    and (
                        now - last_play_time_check < 3600
                        or play_time_new == last_play_time_value
                    )
                ):
                    data["play_time"] = last_play_time_value
                else:
                    if play_time_new is not None:
                        last_play_time_value = play_time_new
                        last_play_time_check = now

                new_day_time = data.get("day_time")

                embed = build_embed(data)

                hourly_counts = await fetch_daily_online_counts(bot.db_pool)

                image_path = save_daily_online_graph(hourly_counts)
                embed.set_image(url=f"attachment://{ONLINE_DAILY_GRAPH_FILENAME}")

                snapshot_data = {
                    "data": data,
                    "counts": hourly_counts,
                    "day_time": new_day_time,
                    "play_time": last_play_time_value,
                    "play_time_check": last_play_time_check,
                }
                last_snapshot = json.dumps(snapshot_data, sort_keys=True)

                file = discord.File(
                    image_path, filename=ONLINE_DAILY_GRAPH_FILENAME
                )

                async for msg in channel.history(
                    limit=config.message_cleanup_limit
                ):
                    if msg.author == bot.user:
                        log_debug(f"[Discord] Удаляем сообщение {msg.id}")
                        try:
                            await msg.delete()
                        except Exception as e:
                            log_debug(
                                f"[Discord] Не удалось удалить сообщение: {e}"
                            )

                log_debug("[Discord] Отправляем сообщение")
                await channel.send(embed=embed, files=[file])

                await asyncio.sleep(config.ftp_poll_interval)
            except asyncio.CancelledError:
                log_debug("[TASK] ftp_polling_task cancelled")
                break
            except Exception as e:
                log_debug(f"[TASK] ftp_polling_task error: {e}")
                await asyncio.sleep(5)


async def save_online_history_task(bot: discord.Client) -> None:
    """Сохраняет список онлайн-игроков в строго заданные минуты часа."""
    log_debug("[TASK] Запущен save_online_history_task")
    await bot.wait_until_ready()
    timeout = aiohttp.ClientTimeout(total=config.http_timeout)
    step = config.online_slice_minutes
    async with aiohttp.ClientSession(timeout=timeout) as session:
        while not bot.is_closed():
            try:
                now = get_moscow_datetime()
                minute = now.minute
                log_debug(f"[ONLINE] Текущее время: {now.strftime('%Y-%m-%d %H:%M:%S')}")

                if minute % step == 0:
                    start_min = now.replace(second=0, microsecond=0)
                    log_debug("[ONLINE] Приступаем к сохранению среза")
                    try:
                        exists = await bot.db_pool.fetchval(
                            "SELECT 1 FROM player_online_history WHERE check_time >= $1 AND check_time < $2 LIMIT 1",
                            start_min,
                            start_min + timedelta(minutes=1),
                        )
                    except Exception as db_e:
                        log_debug(f"[DB] Ошибка проверки истории: {db_e}")
                        exists = True

                    if exists:
                        log_debug("[ONLINE] Срез уже был, пропускаем")
                    else:
                        xml = await fetch_dedicated_server_stats_cached(session)
                        players = parse_players_online(xml) if xml else []
                        log_debug(f"[ONLINE] Игроки онлайн: {players}")
                        records = [
                            (name, now.replace(tzinfo=None)) for name in players
                        ]
                        if records:
                            try:
                                await bot.db_pool.executemany(
                                    """
                                    INSERT INTO player_online_history (
                                        player_name, check_time, date, hour, dow
                                    ) VALUES (
                                        $1, $2, DATE($2), EXTRACT(HOUR FROM $2), EXTRACT(DOW FROM $2)
                                    )
                                    """,
                                    records,
                                )
                                log_debug(f"[DB] Добавлено записей: {len(records)}")
                            except Exception as db_e:
                                log_debug(f"[DB] Ошибка записи игрока: {db_e}")

                next_slice = (
                    now.replace(second=0, microsecond=0)
                    + timedelta(minutes=step - (now.minute % step))
                )
                wait_seconds = (next_slice - get_moscow_datetime()).total_seconds()
                if wait_seconds <= 0:
                    wait_seconds = 1
                log_debug(f"[ONLINE] Ждём {wait_seconds} секунд до следующего среза")
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
