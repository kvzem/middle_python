import sqlite3
from contextlib import contextmanager
import db_entities
from sqlite_data_converter import *

@contextmanager
def conn_context(db_path: str):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    yield conn
    conn.close()


# Задаём путь к файлу с базой данных
db_path = 'db.sqlite'
PAGE_SIZE = 5000


def exec_any_sql(sql):
    with conn_context(db_path) as conn:
        curs = conn.cursor()
        curs.execute(sql)
        data = curs.fetchall()
        return data


def read_all_from_table(table, limit, offset):
    return exec_any_sql(f"SELECT * FROM {table} ORDER BY id LIMIT {limit} OFFSET {offset}".format(table=table,
                                                                                                  limit=limit,
                                                                                                  offset=offset))


def read_person():
    return get_with_limit("person", sqlite_person_to_dataclass)


def read_film_work():
    return get_with_limit("film_work", sqlite_film_work_to_dataclass)


def read_genre():
    return get_with_limit("genre", sqlite_genre_to_dataclass)


def read_person_film_work():
    return get_with_limit("person_film_work", sqlite_person_film_work_to_dataclass)


def read_genre_film_work():
    return get_with_limit("genre_film_work", sqlite_genre_film_work_to_dataclass)


def get_with_limit(table, convertion_method):
    offset = 0
    result = []
    while True:
        part = read_all_from_table(table, PAGE_SIZE, offset)
        if not part:
            break
        else:
            converted = [convertion_method(item) for item in part]
            result.extend(converted)
            offset = offset + PAGE_SIZE
    return result
