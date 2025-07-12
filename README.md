# Farming Simulator Discord Bot

Небольшой Discord-бот для отслеживания состояния сервера Farming Simulator.

Требуется **Python 3.10+**.

## Запуск локально

1. Скопируйте `.env.example` в `.env` и укажите параметры подключения.
2. Установите зависимости командой `pip install -r requirements.txt`.
3. Запустите `python main.py`.

## Переменные окружения

- `DISCORD_TOKEN` — токен Discord-бота
- `DISCORD_CHANNEL_ID` — ID канала для обновления сообщения
- `API_POLL_INTERVAL` — интервал опроса API (сек)
- `FTP_POLL_INTERVAL` — интервал опроса FTP (сек)
- `API_BASE_URL` — базовый URL API
- `API_SECRET_CODE` — секретный код API
- `FTP_HOST` — адрес FTP-сервера
- `FTP_PORT` — порт FTP-сервера
- `FTP_USER` — имя пользователя FTP
- `FTP_PASS` — пароль FTP
- `POSTGRES_URL` — строка подключения к PostgreSQL
- `TOTAL_TOP_LIMIT` — максимальное число игроков в команде `/top_total`

## Railway

Приложение готово для размещения на Railway. Используется `Procfile` с командой `worker: python main.py`.

## Команды

- `/online_month` — график онлайна по дням за последние 30 дней.
- `/top7week` — список самых активных игроков за неделю.
- `/top_total` — общий топ игроков по времени на сервере.


[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

This project is licensed under the [MIT License](LICENSE) — © 2025 KoLeDzHeO (Discord: 288650612051017730).
