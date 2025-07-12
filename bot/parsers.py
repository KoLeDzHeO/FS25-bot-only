import xml.etree.ElementTree as ET
from typing import Any, Dict, Optional, Tuple

from utils.logger import log_debug


def parse_server_stats(
    xml_text: str,
) -> Tuple[Optional[str], Optional[str], Optional[int], Optional[int], Optional[str]]:
    """Извлекает общую информацию о сервере."""
    try:
        root = ET.fromstring(xml_text)
        server_elem = root  # <Server> — корень

        server_name = server_elem.get("name")
        map_name = server_elem.get("mapName")

        slots_elem = server_elem.find(".//Slots")
        slots_max = None
        slots_used = None
        if slots_elem is not None:
            cap = slots_elem.get("capacity")
            used = slots_elem.get("numUsed")
            if cap is not None:
                try:
                    slots_max = int(cap)
                except ValueError:
                    pass
            if used is not None:
                try:
                    slots_used = int(used)
                except ValueError:
                    pass

        stats_elem = root.find(".//Stats")
        last_updated = (
            stats_elem.get("saveDateFormatted") if stats_elem is not None else None
        )

        return server_name, map_name, slots_used, slots_max, last_updated
    except Exception as e:
        log_debug(f"[ERROR] parse_server_stats: {e}")
        return None, None, None, None, None


def parse_farm_money(xml_text: str) -> Optional[int]:
    """Получает баланс фермы из careerSavegame.xml на FTP."""
    try:
        root = ET.fromstring(xml_text)
        elem = root.find(".//statistics/money")
        if elem is not None and elem.text:
            try:
                return int(float(elem.text))
            except (ValueError, TypeError):
                return None
        return None
    except Exception as e:
        log_debug(f"[ERROR] parse_farm_money: {e}")
        return None


def _count_vehicles(xml_text: str, farm_id: str) -> Optional[int]:
    """Подсчёт техники в файле vehicles."""
    try:
        root = ET.fromstring(xml_text)
        vehicles = root.findall(".//vehicle")
        if not vehicles:
            return 0

        keywords = ["pallet", "tree", "wood", "object", "trailerWood", "camera"]

        has_farmid = any(v.get("farmId") is not None for v in vehicles)
        if not has_farmid:
            return None

        count = 0
        for v in vehicles:
            if v.get("farmId") == farm_id:
                filename = v.get("filename", "")
                if not any(k in filename for k in keywords):
                    count += 1
        return count
    except Exception as e:
        log_debug(f"[ERROR] _count_vehicles: {e}")
        return None


def parse_farmland(xml_text: str, farm_id: str) -> Tuple[int, int]:
    """Подсчитывает количество полей у фермы."""
    try:
        root = ET.fromstring(xml_text)
        farmlands = root.findall(".//Farmland") or root.findall(".//farmland")
        total = len(farmlands)
        owned = len([f for f in farmlands if f.get("farmId") == farm_id])
        return owned, total
    except Exception as e:
        log_debug(f"[ERROR] parse_farmland: {e}")
        return 0, 0


def parse_players_online(xml_text: str) -> list:
    """Возвращает список ников онлайн-игроков из dedicated-server-stats.xml.

    Если функция возвращает пустой список, проверьте:
      * актуальность файла dedicated-server-stats.xml;
      * вызывается ли эта функция при обновлении статистики (обычно раз в час);
      * нет ли ошибок в логах.
    """
    try:
        root = ET.fromstring(xml_text)
        players = []
        slots = root.find(".//Slots")
        if slots is not None:
            for player in slots.findall("Player"):
                if player.get("isUsed") == "true":
                    name = (player.text or "").strip()
                    if name and name != "-":
                        players.append(name)
        return players
    except Exception as e:
        log_debug(f"[ERROR] parse_players_online: {e}")
        return []


def parse_last_month_profit(xml_text: str) -> Optional[int]:
    """Возвращает округлённую прибыль за последний месяц (day=1) из farms.xml"""
    try:
        root = ET.fromstring(xml_text)
        stats = root.find(".//farm[@farmId='1']/finances/stats[@day='4']")
        if stats is None:
            return None

        profit = 0.0
        for elem in stats:
            try:
                profit += float(elem.text)
            except (ValueError, TypeError):
                pass

        return round(profit)
    except Exception as e:
        log_debug(f"[ERROR] parse_last_month_profit: {e}")
        return None


def parse_all(
    server_stats: str,
    vehicles_api: str,
    career_savegame_ftp: str,
    farmland_ftp: str,
    vehicles_ftp: Optional[str] = None,
    farms_xml: Optional[str] = None,
    dedicated_server_stats: Optional[str] = None,
    farm_id: str = "1",
) -> Dict[str, Any]:
    """Собирает все данные из разных источников и возвращает единую структуру."""
    try:
        server_name, map_name, slots_used, slots_max, _ = parse_server_stats(
            server_stats
        )

        farm_money = parse_farm_money(career_savegame_ftp)

        fields_owned, fields_total = parse_farmland(farmland_ftp, farm_id)

        vehicles_owned = _count_vehicles(vehicles_api, farm_id)
        if vehicles_owned is None and vehicles_ftp is not None:
            vehicles_owned = _count_vehicles(vehicles_ftp, farm_id)
        vehicles_owned = vehicles_owned or 0

        log_debug(f"[PARSE_ALL] Сервер: {server_name}, Карта: {map_name}")

        last_month_profit = (
            parse_last_month_profit(farms_xml) if farms_xml is not None else None
        )

        players_online = []
        if dedicated_server_stats is not None:
            players_online = parse_players_online(dedicated_server_stats)

        return {
            "last_month_profit": last_month_profit,
            "server_name": server_name,
            "map_name": map_name,
            "slots_used": slots_used,
            "slots_max": slots_max,
            "farm_money": farm_money,
            "fields_owned": fields_owned,
            "fields_total": fields_total,
            "vehicles_owned": vehicles_owned,
            "players_online": players_online,
        }
    except Exception as e:
        log_debug(f"[ERROR] parse_all: {e}")
        return {}
