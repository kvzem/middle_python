import sqlite3
from contextlib import contextmanager
from sqlite.sqlite_converter import *


@contextmanager
def sqlite_connection_context(db_path: str):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    yield conn
    conn.close()


class SQLiteConnector:
    def __init__(self, connection, page_size = 5000):
        self.connection = connection
        self.page_size = page_size

    def exec_any_sql(self, sql):
        curs = self.connection.cursor()
        curs.execute(sql)
        data = curs.fetchall()
        return data

    def read_all_from_table(self, table, limit, offset):
        return self.exec_any_sql(f"SELECT * FROM {table} ORDER BY id LIMIT {limit} OFFSET {offset}"
                                 .format(table=table, limit=limit, offset=offset))

    def read_person(self):
        return self.read_next_part("person", sqlite_person_to_dataclass)

    def read_film_work(self):
        return self.read_next_part("film_work", sqlite_film_work_to_dataclass)

    def read_genre(self):
        return self.read_next_part("genre", sqlite_genre_to_dataclass)

    def read_person_film_work(self):
        return self.read_next_part("person_film_work", sqlite_person_film_work_to_dataclass)

    def read_genre_film_work(self):
        return self.read_next_part("genre_film_work", sqlite_genre_film_work_to_dataclass)

    def read_next_part(self, table, convertion_method):
        offset = 0
        while True:
            part = self.read_all_from_table(table, self.page_size, offset)
            if not part:
                break
            else:
                converted = [convertion_method(item) for item in part]
                offset = offset + self.page_size
                yield converted
