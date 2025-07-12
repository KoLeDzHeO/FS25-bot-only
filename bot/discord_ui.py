from typing import Any, Dict

from utils.helpers import get_moscow_time
import discord


def format_money(amount: Any) -> str:
    """Форматирование суммы денег."""
    try:
        amount = int(amount)
    except (ValueError, TypeError):
        return str(amount) if amount is not None else "—"
    return f"{amount:,} $".replace(",", " ")


def format_play_time(value: Any) -> str | None:
    """Форматирование общего времени игры из минут."""
    try:
        total_minutes = int(float(value))
    except (ValueError, TypeError):
        return None
    hours = total_minutes // 60
    return f"🕒 **Общее время игры:** {hours} ч."


def build_embed(data: Dict[str, Any]) -> discord.Embed:
    """Формирует embed по данным, полученным из ``parse_all``."""

    # Извлекаем данные из словаря или ставим прочерк, если их нет
    server_name = data.get("server_name") or "—"
    map_name = data.get("map_name") or "—"
    slots_used = data.get("slots_used")
    slots_max = data.get("slots_max")
    fields_owned = data.get("fields_owned")
    fields_total = data.get("fields_total")
    vehicles_owned = data.get("vehicles_owned")
    last_month_profit = data.get("last_month_profit")
    players_online = data.get("players_online", [])
    day_time_val = data.get("day_time")
    time_scale_val = data.get("time_scale")
    play_time_val = data.get("play_time")

    slots_str = (
        f"{slots_used if slots_used is not None else '—'} /"
        f" {slots_max if slots_max is not None else '—'}"
    )

    # Формируем строку для денег фермы и прибыли за последний месяц
    if last_month_profit is not None:
        sign = "+" if last_month_profit >= 0 else "−"
        formatted_profit = f"{sign}{abs(last_month_profit):,} $".replace(",", " ")
        money_str = (
            f"{format_money(data.get('farm_money'))} / {formatted_profit}"
            " (за последний месяц)"
        )
    else:
        money_str = f"{format_money(data.get('farm_money'))} / —"

    fields_str = (
        f"{fields_owned if fields_owned is not None else '—'} /"
        f" {fields_total if fields_total is not None else '—'}"
    )
    vehicles_str = f"{vehicles_owned if vehicles_owned is not None else '—'}"

    time_str = "—"
    if isinstance(day_time_val, int):
        minutes_total = day_time_val // 60000
        hours = (minutes_total // 60) % 24
        minutes = minutes_total % 60
        time_str = f"{hours:02d}:{minutes:02d}"

    scale_str = "—"
    if time_scale_val is not None:
        try:
            scale_str = f"×{round(float(time_scale_val))}"
        except (ValueError, TypeError):
            pass

    play_time_str = format_play_time(play_time_val)

    # Текст embed'a формируем единой строкой
    lines = [
        data.get("server_status", "—"),
        f"🧷 **Сервер:** {server_name}",
        f"🗺️ **Карта:** {map_name}",
        f"🕒 **Время в игре:** {time_str} ({scale_str})",
    ]
    if play_time_str:
        lines.append(play_time_str)
    lines += [
        f"💰 **Деньги фермы:** {money_str}",
        f"🌾 **Поля во владении:** {fields_str}",
        f"🚜 **Техника:** {vehicles_str} единиц",
        f"👥 **Слоты:** {slots_str}",
        f"👥 **Онлайн:** {', '.join(players_online) if players_online else 'никого нет'}",
    ]
    description = "\n".join(lines)

    embed = discord.Embed(
        title="Состояние сервера Farming Simulator",
        description=description,
        color=discord.Color.green(),
    )

    # Добавляем информацию о времени обновления бота (московское время)
    embed.set_footer(text=f"Последнее обновление: {get_moscow_time()}")

    return embed
