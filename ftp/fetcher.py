"""Utility for fetching files via FTP."""

from typing import Optional

import aioftp

from config.config import config
from utils.logger import log_debug


async def fetch_file(file_name: str) -> Optional[str]:
    """Download a file from the configured FTP server."""
    log_debug(
        f"[FTP] Connecting to {config.ftp_host}:{config.ftp_port} as {config.ftp_user}"
    )
    try:
        async with aioftp.Client.context(
            config.ftp_host,
            config.ftp_port,
            user=config.ftp_user,
            password=config.ftp_pass,
        ) as ftp_client:
            log_debug(f"[FTP] Entering {config.ftp_profile_dir}...")
            await ftp_client.change_directory(config.ftp_profile_dir)
            log_debug(f"[FTP] Entering {config.ftp_savegame_dir}...")
            await ftp_client.change_directory(config.ftp_savegame_dir)
            log_debug(f"[FTP] Downloading file: {file_name}")
            async with ftp_client.download_stream(file_name) as stream:
                buffer = bytearray()
                while True:
                    chunk = await stream.read(8192)
                    if not chunk:
                        break
                    buffer.extend(chunk)
                log_debug(
                    f"[FTP] File {file_name} downloaded. Size: {len(buffer)} bytes"
                )
                return buffer.decode("utf-8")
    except Exception as e:
        log_debug(f"[FTP] ❌ Error downloading file '{file_name}': {e}")
        return None
