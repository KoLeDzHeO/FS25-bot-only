from typing import Any, Dict

import discord

from utils.helpers import get_moscow_time


def format_money(amount: Any) -> str:
    """Форматирование суммы денег."""
    try:
        amount = int(amount)
    except (ValueError, TypeError):
        return str(amount) if amount is not None else "—"
    return f"{amount:,} $".replace(",", " ")


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

    # Текст embed'a формируем единой строкой
    lines = [
        data.get("server_status", "—"),
        f"🧷 **Сервер:** {server_name}",
        f"🗺️ **Карта:** {map_name}",
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
