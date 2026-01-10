import json
import os
from typing import Any

def load_metadata(filepath: str) -> dict[str, Any]:
    """Load metadata from JSON file. If file not found, return empty dict."""
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        return {}

    if not isinstance(data, dict):
        return {}
    return data


def save_metadata(filepath: str, data: dict[str, Any]) -> None:
    """Save metadata to JSON file."""
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _ensure_data_dir() -> None:
    os.makedirs("data", exist_ok=True)


def load_table_data(table_name: str) -> list[dict[str, Any]]:
    """Load rows of table from data/<table>.json. If not found, return empty list."""
    _ensure_data_dir()
    path = os.path.join("data", f"{table_name}.json")
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        return []

    if not isinstance(data, list):
        return []
    return data


def save_table_data(table_name: str, data: list[dict[str, Any]]) -> None:
    """Save rows of table to data/<table>.json."""
    _ensure_data_dir()
    path = os.path.join("data", f"{table_name}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def delete_table_data(table_name: str) -> None:
    """
    Удаляет файл данных таблицы data/<table_name>.json, если он существует.
    Используется при drop_table для полного удаления таблицы.
    """
    path = os.path.join("data", f"{table_name}.json")
    try:
        os.remove(path)
    except FileNotFoundError:
        pass