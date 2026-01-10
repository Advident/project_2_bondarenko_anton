# Примитивная база данных

## Управление таблицами

Запуск:

- `poetry run database`

Команды:

- `create_table <table> <col:type> ...`
- `list_tables`
- `drop_table <table>`
- `help`
- `exit`

Поддерживаемые типы: `int`, `str`, `bool`.

## CRUD-операции

Команды:

- `insert into <table> values (<v1>, <v2>, ...)`
- `select from <table>`
- `select from <table> where <col> = <val>`
- `update <table> set <col> = <val> where <col> = <val>`
- `delete from <table> where <col> = <val>`
- `info <table>`

Примечания:
- `ID` не передаётся в `insert` — генерируется автоматически.
- Строки вводятся в кавычках: `"Sergei"`.
- Булевы значения: `true/false`.

## Декораторы и кэширование

- `handle_db_errors` — централизованная обработка ошибок.
- `confirm_action` — подтверждение опасных операций (удаление таблиц/записей).
- `log_time` — замер времени выполнения операций.
- `select` использует кэширование результатов одинаковых запросов.

## Демонстрация игрового процесса (asciinema)

[![asciinema](https://asciinema.org/a/3TdfPXAjr6FUQOAN)](https://asciinema.org/a/3TdfPXAjr6FUQOAN)