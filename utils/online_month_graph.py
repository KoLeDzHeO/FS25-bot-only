"""Генерация графика уникальных игроков по дням за последние N дней."""

from __future__ import annotations

from datetime import timedelta

from utils.helpers import get_moscow_datetime
from pathlib import Path
from typing import List, Optional

import matplotlib.pyplot as plt

from config.config import (
    ONLINE_MONTH_DAYS,
    ONLINE_MONTH_GRAPH_PATH,
    ONLINE_MONTH_GRAPH_TITLE,
)
from utils.logger import log_debug


def save_monthly_online_graph(dates: List[str], counts: List[int]) -> str:
    """Сохраняет PNG-график уникальных игроков по дням."""

    plt.figure(figsize=(10, 4))
    plt.bar(range(len(counts)), counts, color="tab:blue")

    plt.xticks(ticks=range(len(dates)), labels=dates, rotation=45, ha="right")
    plt.xlim(-0.5, len(dates) - 0.5)

    plt.xlabel("Дата")
    plt.ylabel("Уникальные игроки")
    plt.title(ONLINE_MONTH_GRAPH_TITLE)

    max_val = max(counts) if counts else 0
    tick_count = max(max_val + 1, 6)
    plt.yticks(range(tick_count))

    plt.grid(axis="y", linestyle="--", alpha=0.5)
    plt.tight_layout()

    output_path = Path(ONLINE_MONTH_GRAPH_PATH)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path)
    plt.close()

    return str(output_path)


async def generate_online_month_graph(db_pool) -> Optional[str]:
    """Создаёт PNG-график уникальных игроков по дням."""
    start_time = get_moscow_datetime() - timedelta(days=ONLINE_MONTH_DAYS)
    try:
        rows = await db_pool.fetch(
            """
            SELECT DATE(check_time) AS day,
                   COUNT(DISTINCT LOWER(player_name)) AS count
            FROM player_online_history
            WHERE check_time >= $1
            GROUP BY day
            ORDER BY day
            """,
            start_time,
        )
    except Exception as e:
        log_debug(f"[DB] Error fetching online month data: {e}")
        raise

    if not rows:
        return None

    counts = {row["day"]: row["count"] for row in rows}

    start_date = start_time.date()
    dates = [start_date + timedelta(days=i) for i in range(ONLINE_MONTH_DAYS)]
    values = [counts.get(d, 0) for d in dates]

    try:
        tick_labels = [d.strftime("%d.%m") for d in dates]
        return save_monthly_online_graph(tick_labels, values)
    except Exception as e:
        log_debug(f"[GRAPH] Error building online month graph: {e}")
        raise
