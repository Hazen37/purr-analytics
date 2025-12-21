# src/db.py

"""
Модуль для работы с базой данных PostgreSQL.

Задачи:
- Установить соединение с БД.
- Предоставить удобные функции для выполнения SQL-запросов.
- Спрятать детали psycopg2 в одном месте.
"""

import psycopg2
import psycopg2.extras
from contextlib import contextmanager

from .config import settings

def get_connection():
    """
    Создаёт и возвращает новое соединение с PostgreSQL.

    Важно:
    - В реальном приложении можно использовать пул подключений (например, psycopg2.pool),
      но для простого аналитического скрипта достаточно простого подключения.
    """
    # ВРЕМЕННЫЙ дебаг: покажем, что реально у нас в настройках.
    # print("[db] Подключаемся к БД с параметрами:")
    # print("      host     =", repr(settings.DB_HOST))
    # print("      port     =", repr(settings.DB_PORT))
    # print("      dbname   =", repr(settings.DB_NAME))
    # print("      user     =", repr(settings.DB_USER))
    # print("      password =", "*" * len(settings.DB_PASSWORD))

    conn = psycopg2.connect(
        host=settings.DB_HOST,
        port=settings.DB_PORT,
        dbname=settings.DB_NAME,
        user=settings.DB_USER,
        password=settings.DB_PASSWORD,
    )
    return conn


@contextmanager
def get_cursor(commit: bool = False):
    """
    Контекстный менеджер для работы с курсором.

    Пример использования:
        with get_cursor(commit=True) as cur:
            cur.execute("INSERT ...", (param1, param2))

    Параметр commit:
    - Если True — по завершении блока будет вызван conn.commit()
    - Если False — просто закроем курсор и соединение без commit()
    """
    conn = get_connection()
    # DictCursor позволяет получать строки в виде словаря: row["field_name"]
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    try:
        yield cur
        if commit:
            conn.commit()
    except Exception as e:
        # При ошибке откатываем транзакцию
        conn.rollback()
        print("[db] Ошибка при выполнении SQL:", e)
        raise
    finally:
        # В любом случае закрываем ресурсы
        cur.close()
        conn.close()


def execute_query(query: str, params: tuple | None = None):
    """
    Выполнить запрос без ожидания результата (INSERT/UPDATE/DELETE).

    query  - SQL строка с плейсхолдерами %s
    params - кортеж параметров (или None, если без параметров)
    """
    with get_cursor(commit=True) as cur:
        cur.execute(query, params)


def fetch_all(query: str, params: tuple | None = None):
    """
    Выполнить SELECT и вернуть все строки.

    Возвращает список psycopg2.extras.DictRow,
    по которым можно обращаться как к словарю: row["field_name"].
    """
    with get_cursor(commit=False) as cur:
        cur.execute(query, params)
        rows = cur.fetchall()
    return rows


def fetch_one(query: str, params: tuple | None = None):
    """
    Выполнить SELECT и вернуть одну строку (или None, если пусто).
    """
    with get_cursor(commit=False) as cur:
        cur.execute(query, params)
        row = cur.fetchone()
    return row