"""
decorators.py

Декораторы и замыкание для улучшения качества кода проекта.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from functools import wraps
from typing import Any

import prompt


def handle_db_errors(func: Callable[..., Any]) -> Callable[..., Any]:
    """
    Централизованная обработка ошибок БД.
    Перехватывает типовые ошибки и печатает понятное сообщение.
    """
    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            return func(*args, **kwargs)
        except FileNotFoundError:
            print("Ошибка: Файл данных не найден. Возможно, база данных не инициализирована.")
            return None
        except KeyError as e:
            # e обычно хранит имя таблицы/столбца
            print(f"Ошибка: Таблица или столбец {e} не найден.")
            return None
        except ValueError as e:
            print(f"Ошибка валидации: {e}")
            return None
        except Exception as e:
            print(f"Произошла непредвиденная ошибка: {e}")
            return None

    return wrapper


def confirm_action(action_name: str) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """
    Фабрика декораторов для подтверждения "опасных" операций (удаление и т.п.).
    """
    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            answer = prompt.string(
                f'Вы уверены, что хотите выполнить "{action_name}"? [y/n]: '
            ).strip().lower()
            if answer != "y":
                print("Операция отменена.")
                return None
            return func(*args, **kwargs)

        return wrapper

    return decorator


def log_time(func: Callable[..., Any]) -> Callable[..., Any]:
    """
    Замер времени выполнения функции.
    """
    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        start = time.monotonic()
        result = func(*args, **kwargs)
        elapsed = time.monotonic() - start
        print(f"Функция {func.__name__} выполнилась за {elapsed:.3f} секунд.")
        return result

    return wrapper


def create_cacher() -> Callable[[str, Callable[[], Any]], Any]:
    """
    Замыкание-кэшер: хранит словарь cache в замыкании.

    cache_result(key, value_func):
    - если key уже есть -> возвращает cache[key]
    - иначе вызывает value_func(), сохраняет и возвращает
    """
    cache: dict[str, Any] = {}

    def cache_result(key: str, value_func: Callable[[], Any]) -> Any:
        if key in cache:
            return cache[key]
        value = value_func()
        cache[key] = value
        return value

    return cache_result
