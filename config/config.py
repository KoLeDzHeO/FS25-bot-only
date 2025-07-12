"""Load application configuration from environment variables."""

from dataclasses import dataclass
from pathlib import Path
import os

from dotenv import load_dotenv

load_dotenv()


@dataclass
class Config:
    """Application configuration loaded from environment variables."""

    discord_token: str = os.getenv("DISCORD_TOKEN", "")
    channel_id: int = int(os.getenv("DISCORD_CHANNEL_ID", 0))
    api_base_url: str = os.getenv("API_BASE_URL", "")
    api_secret_code: str = os.getenv("API_SECRET_CODE", "")
    api_poll_interval: int = int(os.getenv("API_POLL_INTERVAL", 900))
    ftp_poll_interval: int = int(os.getenv("FTP_POLL_INTERVAL", 1800))
    ftp_host: str = os.getenv("FTP_HOST", "")
    ftp_port: int = int(os.getenv("FTP_PORT", 21))
    ftp_user: str = os.getenv("FTP_USER", "")
    ftp_pass: str = os.getenv("FTP_PASS", "")
    postgres_url: str = os.getenv("POSTGRES_URL", "")

    ftp_profile_dir: str = os.getenv("FTP_PROFILE_DIR", "1377415")
    ftp_savegame_dir: str = os.getenv("FTP_SAVEGAME_DIR", "config/savegame1")
    timezone_offset: int = int(os.getenv("TIMEZONE_OFFSET", 3))
    output_dir: Path = Path(os.getenv("OUTPUT_DIR", "output"))

    http_timeout: int = int(os.getenv("HTTP_TIMEOUT", 10))
    online_slice_minutes: int = int(os.getenv("ONLINE_HISTORY_SLICE_MINUTES", 15))
    total_time_interval: int = int(os.getenv("TOTAL_TIME_INTERVAL_SECONDS", 3600))
    message_cleanup_limit: int = int(os.getenv("DISCORD_MESSAGE_CLEANUP_LIMIT", 20))


config = Config()

# Cleanup settings
cleanup_history_days = 30
cleanup_task_interval_seconds = 86400

# Graph settings
ONLINE_MONTH_DAYS = 30
ONLINE_MONTH_GRAPH_FILENAME = "online_month_graph.png"
ONLINE_DAILY_GRAPH_FILENAME = "online_daily_graph.png"
ONLINE_MONTH_GRAPH_TITLE = "Онлайн по дням (последние 30 дней)"
ONLINE_DAILY_GRAPH_TITLE = "Количество игроков по часам (сегодня)"

ONLINE_MONTH_GRAPH_PATH = config.output_dir / ONLINE_MONTH_GRAPH_FILENAME
ONLINE_DAILY_GRAPH_PATH = config.output_dir / ONLINE_DAILY_GRAPH_FILENAME

# Weekly top settings
WEEKLY_TOP_LIMIT = int(os.getenv("WEEKLY_TOP_LIMIT", 7))
WEEKLY_TOP_MAX = int(os.getenv("WEEKLY_TOP_MAX", 10))
WEEKLY_TOP_WEEKDAY = int(os.getenv("WEEKLY_TOP_WEEKDAY", 0))
WEEKLY_TOP_HOUR = int(os.getenv("WEEKLY_TOP_HOUR", 12))
WEEKLY_TOP_LAST_TABLE = "weekly_top_last"

# Top total settings
# Ограничение размера топа по общему времени
TOTAL_TOP_LIMIT = int(os.getenv("TOTAL_TOP_LIMIT", 30))
# Название таблицы с суммарным временем
TOTAL_TOP_TABLE = "player_total_time"
