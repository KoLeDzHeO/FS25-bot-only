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
                content = await stream.read()
                log_debug(
                    f"[FTP] File {file_name} downloaded. Size: {len(content)} bytes"
                )
                return content.decode("utf-8")
    except Exception as e:
        log_debug(f"[FTP] ❌ Error downloading file '{file_name}': {e}")
        return None


async def fetch_files(*file_names: str) -> list[Optional[str]]:
    """Download multiple files during a single FTP session."""
    log_debug(
        f"[FTP] Connecting to {config.ftp_host}:{config.ftp_port} as {config.ftp_user}"
    )
    results: list[Optional[str]] = []
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

            for fname in file_names:
                try:
                    log_debug(f"[FTP] Downloading file: {fname}")
                    async with ftp_client.download_stream(fname) as stream:
                        content = await stream.read()
                        log_debug(
                            f"[FTP] File {fname} downloaded. Size: {len(content)} bytes"
                        )
                        results.append(content.decode("utf-8"))
                except Exception as e:
                    log_debug(f"[FTP] ❌ Error downloading file '{fname}': {e}")
                    results.append(None)
    except Exception as e:
        log_debug(f"[FTP] ❌ Error connecting to FTP: {e}")
        results = [None for _ in file_names]

    return results
