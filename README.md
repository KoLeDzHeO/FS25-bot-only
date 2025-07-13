# Farming Simulator Discord Bot

Небольшой Discord-бот для отслеживания состояния сервера Farming Simulator.
Бот еженедельно архивирует топ активных игроков, посмотреть его можно через
команду `/top7lastweek`.

Требуется **Python 3.10+**.

## Запуск локально

1. Скопируйте `.env.example` в `.env` и укажите параметры подключения.
2. Установите зависимости командой `pip install -r requirements.txt`.
3. Создайте таблицы командой `psql -f schema.sql`.
4. Запустите `python main.py`.

## Переменные окружения

- `DISCORD_TOKEN` — токен Discord-бота
- `DISCORD_CHANNEL_ID` — ID канала для обновления сообщения
- `API_POLL_INTERVAL` — интервал опроса API (сек)
- `FTP_POLL_INTERVAL` — интервал опроса FTP (сек). Сообщение обновляется
  каждый раз при выполнении этого цикла.
- `API_BASE_URL` — базовый URL API
- `API_SECRET_CODE` — секретный код API
- `FTP_HOST` — адрес FTP-сервера
- `FTP_PORT` — порт FTP-сервера
- `FTP_USER` — имя пользователя FTP
- `FTP_PASS` — пароль FTP
- `POSTGRES_URL` — строка подключения к PostgreSQL
- `FTP_PROFILE_DIR` — директория профиля на FTP
- `FTP_SAVEGAME_DIR` — директория сохранения на FTP
- `TIMEZONE_OFFSET` — смещение временной зоны (в часах)
- `OUTPUT_DIR` — каталог для выгрузки графиков
- `WEEKLY_TOP_LIMIT` — сколько игроков выводить в недельном топе
- `WEEKLY_TOP_MAX` — максимально брать из базы при расчёте топа
- `WEEKLY_TOP_WEEKDAY` — день недели генерации топа (0=понедельник)
- `WEEKLY_TOP_HOUR` — час запуска архивации топа
- `HTTP_TIMEOUT` — таймаут HTTP-запросов (сек)
- `ONLINE_HISTORY_SLICE_MINUTES` — интервал среза онлайн-статистики
- `TOTAL_TIME_INTERVAL_SECONDS` — интервал обновления общего времени
- `DISCORD_MESSAGE_CLEANUP_LIMIT` — сколько сообщений удалять перед обновлением
- `TOTAL_TOP_LIMIT` — максимальное число игроков в команде `/top_total`

## Railway

Приложение готово для размещения на Railway. Используется `Procfile` с командой `worker: python main.py`.

## Команды

- `/online_month` — график онлайна по дням за последние 30 дней.
- `/top7week` — список самых активных игроков за неделю.
- `/top7lastweek` — архивный топ игроков за прошлую неделю.
- `/top_total` — общий топ игроков по времени на сервере.

