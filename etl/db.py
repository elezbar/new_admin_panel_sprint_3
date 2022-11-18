from functools import wraps
from psycopg2.extras import DictCursor
import psycopg2

from utils import backoff


@backoff(excps=psycopg2.OperationalError)
def conn_context_pg(dsl: dict):
    """
    Декоратор для подключения к базе данных postgres
    :param dsl: параметры postgres database
    :return: результат выполнения функции
    """
    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            conn = psycopg2.connect(**dsl, cursor_factory=DictCursor)
            conn.autocommit = False
            result = func(conn, *args, **kwargs)
            conn.commit()
            conn.close()
            return result
        return inner
    return func_wrapper


def insert_temp_uuid(cursor, key: str, value: list[str]) -> str:
    """
    Функция добавления множества uuid в временную таблицу, для последующего
    использования её в запросах
    :param cursor: курсор
    :param key: ключ перменной типа uuid
    :param value: список значений для добавления
    :return: select возвращающий id по переданному ключу
    """
    cursor.execute("""
        CREATE TEMPORARY TABLE IF NOT EXISTS cache(
            key varchar(255),
            values uuid
        )
    """)
    values = [(key, val) for val in value]
    cursor.executemany("INSERT INTO cache values(%s,%s)", values)
    return f"""
        select cache.values from cache where key = '{key}'
    """
