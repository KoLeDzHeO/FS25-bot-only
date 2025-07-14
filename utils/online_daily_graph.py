"""Генерация суточного графика количества игроков."""

from datetime import timedelta
from pathlib import Path
from typing import List

import matplotlib.pyplot as plt

from config.config import (
    ONLINE_DAILY_GRAPH_PATH,
    ONLINE_DAILY_GRAPH_TITLE,
)
from utils.helpers import get_moscow_datetime
from utils.logger import log_debug


async def fetch_daily_online_counts(db_pool) -> List[int]:
    """Возвращает число уникальных игроков за каждый час последних 24 часов."""

    now = get_moscow_datetime().replace(minute=0, second=0, microsecond=0)
    start = now - timedelta(hours=23)

    try:
        rows = await db_pool.fetch(
            """
            WITH hours AS (
                SELECT generate_series($1, $2, interval '1 hour') AS hour_start
            )
            SELECT h.hour_start AS hour,
                   COUNT(DISTINCT p.player_name) AS count
            FROM hours h
            LEFT JOIN player_online_history p
                   ON p.check_time >= h.hour_start
                  AND p.check_time < h.hour_start + interval '1 hour'
            GROUP BY h.hour_start
            ORDER BY h.hour_start
            """,
            start,
            now,
        )
    except Exception as e:
        log_debug(f"[DB] Error fetching online day data: {e}")
        raise

    return [row["count"] for row in rows]


def save_daily_online_graph(counts: List[int]) -> str:
    """Сохраняет PNG-график количества игроков за последние 24 часа."""

    now = get_moscow_datetime().replace(minute=0, second=0, microsecond=0)
    start = now - timedelta(hours=len(counts) - 1)
    hours = [(start + timedelta(hours=i)).hour for i in range(len(counts))]

    plt.figure(figsize=(10, 3))
    plt.bar(range(len(counts)), counts, color="tab:blue")

    plt.xticks(ticks=range(len(hours)), labels=hours)
    plt.xlim(-0.5, len(hours) - 0.5)

    plt.xlabel("Час")
    plt.ylabel("Игроки")
    plt.title(ONLINE_DAILY_GRAPH_TITLE)

    max_val = max(counts) if counts else 0
    tick_count = max(max_val + 1, 6)
    plt.yticks(range(tick_count))

    plt.grid(axis="y", linestyle="--", alpha=0.5)
    plt.tight_layout()

    output_path = Path(ONLINE_DAILY_GRAPH_PATH)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path)
    plt.close()
    return str(output_path)
