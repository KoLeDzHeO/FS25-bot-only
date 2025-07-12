"""Helpers for fetching files from the API."""

from typing import Optional, Tuple

from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode

import aiohttp

import time

from config.config import config
from utils.logger import log_debug
from ftp.fetcher import fetch_files


def _mask_url_param(url: str, param: str = "code", mask: str = "***") -> str:
    """Return ``url`` with the value of ``param`` replaced by ``mask``."""
    parts = urlsplit(url)
    query = parse_qsl(parts.query, keep_blank_values=True)
    sanitized_query = [(k, mask if k == param else v) for k, v in query]
    new_query = urlencode(sanitized_query)
    sanitized_parts = parts._replace(query=new_query)
    return urlunsplit(sanitized_parts)


async def _fetch(session: aiohttp.ClientSession, url: str, desc: str) -> Optional[str]:
    """Fetch a file from the given ``url`` using the provided session."""
    safe_url = _mask_url_param(url)
    log_debug(f"[API] Загружаем {desc} по адресу: {safe_url}")
    try:
        async with session.get(url) as resp:
            resp.raise_for_status()
            data = await resp.text()
            log_debug(f"[API] {desc} загружен успешно.")
            return data
    except Exception as e:
        log_debug(f"[API] ❌ Ошибка загрузки {desc}: {e}")
        return None


async def fetch_api_file(
    session: aiohttp.ClientSession, filename: str
) -> Optional[str]:
    """Download a file from the API by name."""
    url = f"{config.api_base_url}?file={filename}&code={config.api_secret_code}"
    return await _fetch(session, url, filename)


async def fetch_dedicated_server_stats(session: aiohttp.ClientSession) -> Optional[str]:
    """Download ``dedicated-server-stats.xml`` from the API feed."""
    url = (
        config.api_base_url.replace(
            "dedicated-server-savegame.html",
            "dedicated-server-stats.xml",
        )
        + f"?code={config.api_secret_code}"
    )
    return await _fetch(session, url, "dedicated-server-stats.xml")


_stats_cache: tuple[Optional[str], float] = (None, 0.0)


async def fetch_dedicated_server_stats_cached(
    session: aiohttp.ClientSession, *, ttl: int = 120
) -> Optional[str]:
    """Fetch stats XML with simple in-memory caching."""
    global _stats_cache
    data, ts = _stats_cache
    now = time.monotonic()
    if data is not None and now - ts < ttl:
        log_debug("[API] Using cached dedicated-server-stats.xml")
        return data
    data = await fetch_dedicated_server_stats(session)
    if data:
        _stats_cache = (data, now)
    return data


async def fetch_required_files(
    session: aiohttp.ClientSession,
) -> Tuple[
    Optional[str],
    Optional[str],
    Optional[str],
    Optional[str],
    Optional[str],
    Optional[str],
]:
    """Fetch all files required for building server stats."""
    log_debug("[API] Получаем dedicated-server-stats.xml")
    stats_xml = await fetch_dedicated_server_stats_cached(session)
    log_debug("[API] Получаем vehicles")
    vehicles_xml = await fetch_api_file(session, "vehicles")
    log_debug("[API] Получаем careerSavegame из API")
    career_api_xml = await fetch_api_file(session, "careerSavegame")

    career_ftp, farmland_ftp, farms_ftp = await fetch_files(
        "careerSavegame.xml", "farmland.xml", "farms.xml"
    )

    return (
        stats_xml,
        vehicles_xml,
        career_api_xml,
        career_ftp,
        farmland_ftp,
        farms_ftp,
    )
