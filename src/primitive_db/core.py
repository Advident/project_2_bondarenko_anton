"""
core.py

В этом модуле:
- управление таблицами (создание / удаление / список)
- CRUD-операции над данными (insert / select / update / delete)
- проверка типов значений по схеме таблицы
- декораторы для:
  - централизованной обработки ошибок
  - подтверждения опасных операций
  - логирования времени выполнения "медленных" операций
- кэширование результатов select через замыкание

"""

from __future__ import annotations

from typing import Any

from decorators import confirm_action, create_cacher, handle_db_errors, log_time

from .constants import VALID_TYPES

# Кэшер (замыкание) для select
_CACHER = create_cacher()


def _parse_column_def(col_def: str) -> tuple[str, str]:
    """
    Разбирает определение столбца формата 'name:type' в кортеж (name, type).

    Ошибки:
    - ValueError: если строка не соответствует формату или тип не поддерживается.
    """
    if ":" not in col_def:
        raise ValueError(f"Некорректное значение: {col_def}. Попробуйте снова.")

    name, col_type = col_def.split(":", 1)
    name = name.strip()
    col_type = col_type.strip()

    if not name:
        raise ValueError(f"Некорректное значение: {col_def}. Попробуйте снова.")
    if col_type not in VALID_TYPES:
        raise ValueError(f"Некорректное значение: {col_type}. Попробуйте снова.")

    return name, col_type


def _get_schema(metadata: dict[str, Any], table_name: str) -> list[dict[str, str]]:
    """
    Возвращает схему таблицы из metadata.

    Ошибки:
    - KeyError: если таблица не существует
    - ValueError: если схема повреждена
    """
    if table_name not in metadata:
        raise KeyError(table_name)

    cols = metadata[table_name].get("columns")
    if not isinstance(cols, list) or not cols:
        raise ValueError("Некорректное значение: схема таблицы. Попробуйте снова.")
    return cols


def _schema_to_str(schema: list[dict[str, str]]) -> str:
    """Преобразует схему таблицы в строку для вывода пользователю."""
    return ", ".join([f'{c["name"]}:{c["type"]}' for c in schema])


def _validate_value(value: Any, expected_type: str) -> None:
    """
    Проверяет, что значение соответствует ожидаемому типу из схемы.

    Поддерживаемые типы: int, str, bool.
    Ошибки:
    - ValueError: если тип не совпал
    """
    if expected_type == "int" and not isinstance(value, int):
        raise ValueError(f"Некорректное значение: {value}. Попробуйте снова.")
    if expected_type == "str" and not isinstance(value, str):
        raise ValueError(f"Некорректное значение: {value}. Попробуйте снова.")
    if expected_type == "bool" and not isinstance(value, bool):
        raise ValueError(f"Некорректное значение: {value}. Попробуйте снова.")


def _validate_column_exists(schema: list[dict[str, str]], column_name: str) -> dict[str, str]:
    """
    Проверяет, что столбец существует в схеме.
    Возвращает описание столбца (dict {'name':..., 'type':...}).

    Ошибки:
    - KeyError: если столбца нет
    """
    for col in schema:
        if col["name"] == column_name:
            return col
    raise KeyError(column_name)


def _table_version(table_data: list[dict[str, Any]]) -> str:
    """
    Небольшая "версия таблицы" для кэширования select.
    Если данные изменились (insert/update/delete), версия меняется, а кэш не мешает.
    """
    ids = [row.get("ID") for row in table_data if isinstance(row, dict)]
    max_id = max([i for i in ids if isinstance(i, int)], default=0)
    return f"{len(table_data)}:{max_id}"


@handle_db_errors
def create_table(metadata: dict[str, Any], table_name: str, columns: list[str]) -> dict[str, Any] | None:
    """
    Создаёт таблицу и добавляет её схему в metadata.

    Правила:
    - Если таблица уже существует — печатает ошибку и возвращает metadata без изменений.
    - Автоматически добавляет столбец ID:int первым.
    - Проверяет корректность типов: int, str, bool.
    - Столбец ID нельзя задавать вручную.

    Возвращает:
    - обновлённый metadata (dict)
    - None, если произошла ошибка (обработается декоратором)
    """
    if table_name in metadata:
        print(f'Ошибка: Таблица "{table_name}" уже существует.')
        return metadata

    parsed_cols: list[tuple[str, str]] = []
    for col in columns:
        name, col_type = _parse_column_def(col)
        if name == "ID":
            raise ValueError("Некорректное значение: ID. Попробуйте снова.")
        parsed_cols.append((name, col_type))

    schema: list[tuple[str, str]] = [("ID", "int"), *parsed_cols]
    metadata[table_name] = {"columns": [{"name": n, "type": t} for n, t in schema]}

    schema_str = ", ".join([f"{n}:{t}" for n, t in schema])
    print(f'Таблица "{table_name}" успешно создана со столбцами: {schema_str}')
    return metadata


@confirm_action("удаление таблицы")
@handle_db_errors
def drop_table(metadata: dict[str, Any], table_name: str) -> dict[str, Any] | None:
    """
    Удаляет таблицу из metadata.

    Поведение:
    - Если таблицы нет — печатает ошибку.
    - Иначе удаляет и печатает подтверждение.

    Важно:
    - Перед выполнением запрашивается подтверждение (декоратор confirm_action).
    """
    if table_name not in metadata:
        print(f'Ошибка: Таблица "{table_name}" не существует.')
        return metadata

    del metadata[table_name]
    print(f'Таблица "{table_name}" успешно удалена.')
    return metadata


@handle_db_errors
def list_tables(metadata: dict[str, Any]) -> None:
    """
    Печатает список всех таблиц.
    Если таблиц нет — печатает "(таблиц нет)".
    """
    if not metadata:
        print("(таблиц нет)")
        return

    for name in sorted(metadata.keys()):
        print(f"- {name}")


@log_time
@handle_db_errors
def insert(
    metadata: dict[str, Any],
    table_name: str,
    values: list[Any],
    table_data: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], int] | None:
    """
    Добавляет запись в таблицу.

    Правила:
    - Таблица должна существовать (иначе KeyError).
    - Количество значений должно совпадать с числом столбцов без ID.
    - Типы значений валидируются по схеме.
    - ID генерируется автоматически: max(existing_id) + 1.

    Возвращает:
    - (обновлённые данные таблицы, новый ID)
    - None при ошибке (обработает декоратор)
    """
    schema = _get_schema(metadata, table_name)

    expected_count = len(schema) - 1  # ID не вводится пользователем
    if len(values) != expected_count:
        raise ValueError("Некорректное значение: values. Попробуйте снова.")

    # Проверяем типы значений по схеме (пропускаем ID)
    for idx, col in enumerate(schema[1:]):
        _validate_value(values[idx], col["type"])

    existing_ids = [row.get("ID") for row in table_data if isinstance(row, dict)]
    max_id = max([i for i in existing_ids if isinstance(i, int)], default=0)
    new_id = max_id + 1

    row: dict[str, Any] = {"ID": new_id}
    for idx, col in enumerate(schema[1:]):
        row[col["name"]] = values[idx]

    table_data.append(row)
    return table_data, new_id


@log_time
@handle_db_errors
def select(
    metadata: dict[str, Any],
    table_name: str,
    table_data: list[dict[str, Any]],
    where_clause: dict[str, Any] | None = None,
) -> list[dict[str, Any]] | None:
    """
    Возвращает записи таблицы. Поддерживает фильтрацию через where_clause.

    where_clause:
    - None -> вернуть все строки
    - {'age': 28} -> вернуть строки, где age == 28

    Дополнительно:
    - результат кэшируется (через замыкание), чтобы одинаковые запросы работали быстрее.
    - кэш учитывает "версию таблицы" (len + max(ID)), чтобы не мешать после изменений.

    Возвращает:
    - список строк (list[dict])
    - None при ошибке (обработает декоратор)
    """
    schema = _get_schema(metadata, table_name)

    version = _table_version(table_data)
    if where_clause is None:
        cache_key = f"{table_name}|{version}|ALL"
    else:
        (col, val), = where_clause.items()
        cache_key = f"{table_name}|{version}|WHERE:{col}={repr(val)}"

    def compute() -> list[dict[str, Any]]:
        if where_clause is None:
            return list(table_data)

        (col, val), = where_clause.items()
        # Валидация: столбец должен существовать
        col_def = _validate_column_exists(schema, col)
        _validate_value(val, col_def["type"])

        return [row for row in table_data if isinstance(row, dict) and row.get(col) == val]

    return _CACHER(cache_key, compute)


@handle_db_errors
def update(
    metadata: dict[str, Any],
    table_name: str,
    table_data: list[dict[str, Any]],
    set_clause: dict[str, Any],
    where_clause: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[int]] | None:
    """
    Обновляет записи, подходящие под where_clause, выставляя поля из set_clause.

    Пример:
    - set_clause = {'age': 29}
    - where_clause = {'name': 'Sergei'}

    Правила:
    - Столбцы в where/set должны существовать в схеме.
    - Значения проверяются по типам.
    - Обновлять ID запрещено.

    Возвращает:
    - (обновлённые данные, список ID обновлённых строк)
    - None при ошибке (обработает декоратор)
    """
    schema = _get_schema(metadata, table_name)

    (w_col, w_val), = where_clause.items()
    (s_col, s_val), = set_clause.items()

    if s_col == "ID":
        raise ValueError("Некорректное значение: ID. Попробуйте снова.")

    w_def = _validate_column_exists(schema, w_col)
    s_def = _validate_column_exists(schema, s_col)

    _validate_value(w_val, w_def["type"])
    _validate_value(s_val, s_def["type"])

    updated_ids: list[int] = []
    for row in table_data:
        if not isinstance(row, dict):
            continue
        if row.get(w_col) == w_val:
            row[s_col] = s_val
            if isinstance(row.get("ID"), int):
                updated_ids.append(row["ID"])

    return table_data, updated_ids


@confirm_action("удаление записи")
@handle_db_errors
def delete(
    metadata: dict[str, Any],
    table_name: str,
    table_data: list[dict[str, Any]],
    where_clause: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[int]] | None:
    """
    Удаляет записи, подходящие под where_clause.

    Пример:
    - where_clause = {'ID': 1}

    Правила:
    - Столбец в where должен существовать в схеме.
    - Значение проверяется по типу.

    Возвращает:
    - (новые данные, список ID удалённых строк)
    - None при ошибке (обработает декоратор)

    Важно:
    - Перед выполнением запрашивается подтверждение (confirm_action).
    """
    schema = _get_schema(metadata, table_name)

    (w_col, w_val), = where_clause.items()
    w_def = _validate_column_exists(schema, w_col)
    _validate_value(w_val, w_def["type"])

    new_data: list[dict[str, Any]] = []
    deleted_ids: list[int] = []

    for row in table_data:
        if not isinstance(row, dict):
            continue
        if row.get(w_col) == w_val:
            if isinstance(row.get("ID"), int):
                deleted_ids.append(row["ID"])
            continue
        new_data.append(row)

    return new_data, deleted_ids


@handle_db_errors
def info(metadata: dict[str, Any], table_name: str, table_data: list[dict[str, Any]]) -> str | None:
    """
    Возвращает текстовую информацию о таблице:
    - имя таблицы
    - список столбцов с типами
    - количество записей
    """
    schema = _get_schema(metadata, table_name)
    cols_str = _schema_to_str(schema)
    return f'Таблица: {table_name}\nСтолбцы: {cols_str}\nКоличество записей: {len(table_data)}'
