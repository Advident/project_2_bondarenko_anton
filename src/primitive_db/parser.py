import ast
import re
from typing import Any


_WHERE_RE = re.compile(r"^\s*(?P<col>\w+)\s*=\s*(?P<val>.+?)\s*$")
_SET_RE = re.compile(r"^\s*(?P<col>\w+)\s*=\s*(?P<val>.+?)\s*$")


def _parse_scalar(value_str: str) -> Any:
    """
    Parse scalar value:
    - "text" or 'text' -> str
    - 123 -> int
    - true/false -> bool
    """
    s = value_str.strip()

    low = s.lower()
    if low == "true":
        return True
    if low == "false":
        return False

    # quoted strings, numbers, etc.
    try:
        return ast.literal_eval(s)
    except (SyntaxError, ValueError):
        # fallback: raw string (but per task лучше требовать кавычки)
        return s


def parse_where_clause(expr: str) -> dict[str, Any]:
    """Turn 'age = 28' into {'age': 28}."""
    m = _WHERE_RE.match(expr)
    if not m:
        raise ValueError(f"Некорректное значение: {expr}. Попробуйте снова.")
    col = m.group("col")
    val = _parse_scalar(m.group("val"))
    return {col: val}


def parse_set_clause(expr: str) -> dict[str, Any]:
    """Turn 'age = 29' into {'age': 29}."""
    m = _SET_RE.match(expr)
    if not m:
        raise ValueError(f"Некорректное значение: {expr}. Попробуйте снова.")
    col = m.group("col")
    val = _parse_scalar(m.group("val"))
    return {col: val}


def parse_values_list(values_part: str) -> list:
    """
    Parse values from string like:
    ("Sergei", 28, true)
    """
    s = values_part.strip()

    if not (s.startswith("(") and s.endswith(")")):
        raise ValueError(f"Некорректное значение: {values_part}. Попробуйте снова.")

    inner = s[1:-1].strip()
    if not inner:
        return []

    parts = [p.strip() for p in inner.split(",")]
    values: list = []

    for part in parts:
        low = part.lower()

        # bool
        if low == "true":
            values.append(True)
            continue
        if low == "false":
            values.append(False)
            continue

        # int
        if part.isdigit():
            values.append(int(part))
            continue

        # str — строго в кавычках
        if (
            (part.startswith('"') and part.endswith('"'))
            or (part.startswith("'") and part.endswith("'"))
        ):
            values.append(part[1:-1])
            continue

        raise ValueError(f"Некорректное значение: {part}. Попробуйте снова.")

    return values
