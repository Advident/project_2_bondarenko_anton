"""
engine.py
Главный модуль взаимодействия с пользователем.

Отвечает за:
- запуск программы
- игровой (REPL) цикл
- разбор команд пользователя
- вызов бизнес-логики из core.py
- загрузку / сохранение данных через utils.py
"""

import re
import shlex

import prompt
from prettytable import PrettyTable

from .constants import META_FILE
from .core import (
    create_table,
    delete,
    drop_table,
    info,
    insert,
    list_tables,
    select,
    update,
)
from .parser import parse_set_clause, parse_values_list, parse_where_clause
from .utils import load_metadata, load_table_data, save_metadata, save_table_data


def print_help() -> None:
    """Печатает справку по доступным командам."""
    print("\n***Операции с данными***\n")

    print("CRUD-команды:")
    print('<command> insert into <table> values (<v1>, <v2>, ...) - создать запись')
    print('<command> select from <table> - прочитать все записи')
    print('<command> select from <table> where <col> = <val> - прочитать по условию')
    print('<command> update <table> set <col> = <val> where <col> = <val> - обновить запись')
    print('<command> delete from <table> where <col> = <val> - удалить запись')
    print('<command> info <table> - информация о таблице')

    print("\nУправление таблицами:")
    print('<command> create_table <table> <col:type> ... - создать таблицу')
    print('<command> list_tables - список таблиц')
    print('<command> drop_table <table> - удалить таблицу')

    print("\nОбщие команды:")
    print("<command> help - справка")
    print("<command> exit - выход\n")


def _print_table(rows: list[dict], columns: list[str]) -> None:
    """Выводит список словарей в виде таблицы PrettyTable."""
    table = PrettyTable()
    table.field_names = columns

    for row in rows:
        table.add_row([row.get(col) for col in columns])

    print(table)


def run() -> None:
    """Основной цикл программы (REPL)."""
    print_help()

    while True:
        user_input = prompt.string(">>>Введите команду: ").strip()
        if not user_input:
            continue

        raw = user_input
        low = raw.lower()

        # Загружаем метаданные при каждой итерации
        metadata = load_metadata(META_FILE)

        # Общие команды
        if low == "exit":
            return

        if low == "help":
            print_help()
            continue

        # CRUD-КОМАНДЫ
        # ---------- INSERT ----------
        if low.startswith("insert "):
            match = re.match(
                r'^\s*insert\s+into\s+(\w+)\s+values\s*(\(.*\))\s*$',
                raw,
                flags=re.IGNORECASE,
            )
            if not match:
                print("Некорректное значение: insert. Попробуйте снова.")
                continue

            table_name = match.group(1)
            values_part = match.group(2)

            try:
                values = parse_values_list(values_part)
            except ValueError as e:
                print(e)
                continue

            table_data = load_table_data(table_name)

            try:
                table_data, new_id = insert(metadata, table_name, values, table_data)
            except KeyError:
                print(f'Ошибка: Таблица "{table_name}" не существует.')
                continue
            except ValueError as e:
                print(e)
                continue

            save_table_data(table_name, table_data)
            print(f'Запись с ID={new_id} успешно добавлена в таблицу "{table_name}".')
            continue

        # ---------- SELECT ----------
        if low.startswith("select "):
            match = re.match(
                r'^\s*select\s+from\s+(\w+)(?:\s+where\s+(.*))?$',
                raw,
                flags=re.IGNORECASE,
            )
            if not match:
                print("Некорректное значение: select. Попробуйте снова.")
                continue

            table_name = match.group(1)
            where_expr = match.group(2)

            table_data = load_table_data(table_name)

            where_clause = None
            if where_expr:
                try:
                    where_clause = parse_where_clause(where_expr)
                except ValueError as e:
                    print(e)
                    continue

            try:
                rows = select(table_data, where_clause)
            except ValueError as e:
                print(e)
                continue

            try:
                columns = [c["name"] for c in metadata[table_name]["columns"]]
            except KeyError:
                print(f'Ошибка: Таблица "{table_name}" не существует.')
                continue

            if not rows:
                print("(нет данных)")
                continue

            _print_table(rows, columns)
            continue

        # ---------- UPDATE ----------
        if low.startswith("update "):
            match = re.match(
                r'^\s*update\s+(\w+)\s+set\s+(.*?)\s+where\s+(.*)$',
                raw,
                flags=re.IGNORECASE,
            )
            if not match:
                print("Некорректное значение: update. Попробуйте снова.")
                continue

            table_name = match.group(1)
            set_expr = match.group(2)
            where_expr = match.group(3)

            try:
                set_clause = parse_set_clause(set_expr)
                where_clause = parse_where_clause(where_expr)
            except ValueError as e:
                print(e)
                continue

            table_data = load_table_data(table_name)
            table_data, updated_ids = update(table_data, set_clause, where_clause)
            save_table_data(table_name, table_data)

            if updated_ids:
                print(f'Запись с ID={updated_ids[0]} в таблице "{table_name}" успешно обновлена.')
            else:
                print("(ничего не обновлено)")
            continue

        # ---------- DELETE ----------
        if low.startswith("delete "):
            match = re.match(
                r'^\s*delete\s+from\s+(\w+)\s+where\s+(.*)$',
                raw,
                flags=re.IGNORECASE,
            )
            if not match:
                print("Некорректное значение: delete. Попробуйте снова.")
                continue

            table_name = match.group(1)
            where_expr = match.group(2)

            try:
                where_clause = parse_where_clause(where_expr)
            except ValueError as e:
                print(e)
                continue

            table_data = load_table_data(table_name)
            table_data, deleted_ids = delete(table_data, where_clause)
            save_table_data(table_name, table_data)

            if deleted_ids:
                print(f'Запись с ID={deleted_ids[0]} успешно удалена из таблицы "{table_name}".')
            else:
                print("(ничего не удалено)")
            continue

        # ---------- INFO ----------
        if low.startswith("info "):
            parts = raw.split()
            if len(parts) != 2:
                print("Некорректное значение: info. Попробуйте снова.")
                continue

            table_name = parts[1]
            table_data = load_table_data(table_name)

            try:
                print(info(metadata, table_name, table_data))
            except KeyError:
                print(f'Ошибка: Таблица "{table_name}" не существует.')
            continue

        # КОМАНДЫ УПРАВЛЕНИЯ ТАБЛИЦАМИ (через shlex)
        try:
            args = shlex.split(raw)
        except ValueError:
            print("Некорректное значение: команда. Попробуйте снова.")
            continue

        cmd = args[0]

        if cmd == "list_tables":
            list_tables(metadata)
            continue

        if cmd == "create_table":
            if len(args) < 3:
                print("Некорректное значение: create_table. Попробуйте снова.")
                continue

            table_name = args[1]
            columns = args[2:]

            try:
                metadata = create_table(metadata, table_name, columns)
            except ValueError as e:
                print(e)
                continue

            save_metadata(META_FILE, metadata)
            continue

        if cmd == "drop_table":
            if len(args) != 2:
                print("Некорректное значение: drop_table. Попробуйте снова.")
                continue

            metadata = drop_table(metadata, args[1])
            save_metadata(META_FILE, metadata)
            continue

        # ------------------------------------------------------------
        print(f"Функции {cmd} нет. Попробуйте снова.")
