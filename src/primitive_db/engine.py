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

from __future__ import annotations

import re
import shlex
from typing import Any

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


def _print_table(rows: list[dict[str, Any]], columns: list[str]) -> None:
    """
    Печатает строки (list[dict]) через PrettyTable.
    columns задаёт порядок вывода столбцов.
    """
    table = PrettyTable()
    table.field_names = columns

    for row in rows:
        table.add_row([row.get(col) for col in columns])

    print(table)


def run() -> None:
    """Основной цикл программы."""
    print_help()

    while True:
        user_input = prompt.string(">>>Введите команду: ").strip()
        if not user_input:
            continue

        raw = user_input
        low = raw.lower()

        # Метаданные загружаем на каждой итерации: так мы видим актуальное состояние
        metadata = load_metadata(META_FILE)

        # --------------------
        # Общие команды
        # --------------------
        if low == "exit":
            return

        if low == "help":
            print_help()
            continue

        # ============================================================
        # CRUD-команды
        # ============================================================

        # ---------- INSERT ----------
        if low.startswith("insert "):
            # Пример: insert into users values ("Sergei", 28, true)
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

            # insert обёрнут декораторами -> может вернуть None
            result = insert(
                metadata=metadata,
                table_name=table_name,
                values=values,
                table_data=table_data,
            )
            if result is None:
                continue

            table_data, new_id = result
            save_table_data(table_name, table_data)
            print(f'Запись с ID={new_id} успешно добавлена в таблицу "{table_name}".')
            continue

        # ---------- SELECT ----------
        if low.startswith("select "):
            # Пример:
            # select from users
            # select from users where age = 28
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

            where_clause = None
            if where_expr:
                try:
                    where_clause = parse_where_clause(where_expr)
                except ValueError as e:
                    print(e)
                    continue

            table_data = load_table_data(table_name)

            # select обёрнут декораторами -> может вернуть None
            rows = select(
                metadata=metadata,
                table_name=table_name,
                table_data=table_data,
                where_clause=where_clause,
            )
            if rows is None:
                # Ошибка уже выведена декоратором handle_db_errors
                continue

            if not rows:
                print("(нет данных)")
                continue

            # Порядок столбцов берём из схемы в metadata
            try:
                columns = [c["name"] for c in metadata[table_name]["columns"]]
            except KeyError:
                # На всякий случай, хотя core/select уже должен был сообщить
                print(f'Ошибка: Таблица "{table_name}" не существует.')
                continue

            _print_table(rows, columns)
            continue

        # ---------- UPDATE ----------
        if low.startswith("update "):
            # Пример: update users set age = 29 where name = "Sergei"
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

            # update обёрнут декораторами -> может вернуть None
            result = update(
                metadata=metadata,
                table_name=table_name,
                table_data=table_data,
                set_clause=set_clause,
                where_clause=where_clause,
            )
            if result is None:
                continue

            table_data, updated_ids = result
            save_table_data(table_name, table_data)

            if updated_ids:
                print(f'Запись с ID={updated_ids[0]} в таблице "{table_name}" успешно обновлена.')
            else:
                print("(ничего не обновлено)")
            continue

        # ---------- DELETE ----------
        if low.startswith("delete "):
            # Пример: delete from users where ID = 1
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

            # delete обёрнут confirm_action + handle_db_errors -> может вернуть None
            result = delete(
                metadata=metadata,
                table_name=table_name,
                table_data=table_data,
                where_clause=where_clause,
            )
            if result is None:
                # либо ошибка, либо пользователь отменил операцию
                continue

            table_data, deleted_ids = result
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

            # info обёрнут декораторами -> может вернуть None
            result = info(metadata, table_name, table_data)
            if result is None:
                continue

            print(result)
            continue

        # ============================================================
        # Команды управления таблицами (можно через shlex)
        # ============================================================
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

            result = create_table(metadata, table_name, columns)
            if result is None:
                continue

            save_metadata(META_FILE, result)
            continue

        if cmd == "drop_table":
            if len(args) != 2:
                print("Некорректное значение: drop_table. Попробуйте снова.")
                continue

            result = drop_table(metadata, args[1])
            if result is None:
                # пользователь мог отменить удаление
                continue

            save_metadata(META_FILE, result)
            continue

        # Если команда не распознана
        print(f"Функции {cmd} нет. Попробуйте снова.")
