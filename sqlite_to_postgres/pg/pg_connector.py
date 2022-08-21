from psycopg2.extras import execute_batch
from pg.pg_converter import pg_person_to_dataclass, pg_genre_to_dataclass, pg_film_work_to_dataclass, \
    pg_genre_film_work_to_dataclass, pg_person_film_work_to_dataclass


class PostgresConnector:
    def __init__(self, connection, page_size = 5000):
        self.connection = connection
        self.page_size = page_size

    def write_person(self, people):
        with self.connection.cursor() as cur:
            query = """INSERT INTO person (id, full_name, created, modified)
                       VALUES (%s, %s, %s, %s) ON CONFLICT do nothing"""
            data = [(item.id, item.full_name, item.created, item.modified) for item in people]
            execute_batch(cur, query, data, page_size=self.page_size)

    def write_film_work(self, film_works):
        with self.connection.cursor() as cur:
            query = """INSERT INTO film_work (id, title, description, creation_date, rating, type, created, modified)
                                      VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT do nothing"""
            data = [(item.id, item.title, item.description, item.creation_date,
                     item.rating, item.type, item.created, item.modified) for item in film_works]
            execute_batch(cur, query, data, page_size=self.page_size)

    def write_genre(self, genres):
        with self.connection.cursor() as cur:
            query = """INSERT INTO genre (id, name, description, created, modified)
                                  VALUES (%s, %s, %s, %s, %s) ON CONFLICT do nothing"""
            data = [(item.id, item.name, item.description, item.created, item.modified) for item in genres]
            execute_batch(cur, query, data, page_size=self.page_size)

    def write_genre_film_work(self, genre_film_work):
        with self.connection.cursor() as cur:
            query = """INSERT INTO genre_film_work (id, genre_id, film_work_id, created)
                                            VALUES (%s, %s, %s, %s) ON CONFLICT do nothing"""
            data = [(item.id, item.genre_id, item.film_work_id, item.created) for item in genre_film_work]
            execute_batch(cur, query, data, page_size=self.page_size)

    def write_person_film_work(self, genre_film_work):
        with self.connection.cursor() as cur:
            query = """INSERT INTO person_film_work (id, person_id, film_work_id, role, created)
                                             VALUES (%s, %s, %s, %s, %s) ON CONFLICT do nothing"""
            data = [(item.id, item.person_id, item.film_work_id, item.role, item.created) for item in genre_film_work]
            execute_batch(cur, query, data, page_size=self.page_size)

    def read_table_and_convert(self, table, convertion_func):
        with self.connection.cursor() as cur:
            offset = 0
            while True:
                sql = f"SELECT * FROM {table} ORDER BY id LIMIT {self.page_size} OFFSET {offset}"
                cur.execute(sql)
                part = cur.fetchmany(self.page_size)
                if not part:
                    break
                else:
                    converted = [convertion_func(item) for item in part]
                    offset = offset + self.page_size
                    yield converted

    def read_person(self):
        return self.read_table_and_convert('content.person', pg_person_to_dataclass)

    def read_film_work(self):
        return self.read_table_and_convert('content.film_work', pg_film_work_to_dataclass)

    def read_genre(self):
        return self.read_table_and_convert('content.genre', pg_genre_to_dataclass)

    def read_person_film_work(self):
        return self.read_table_and_convert('content.person_film_work', pg_person_film_work_to_dataclass)

    def read_genre_film_work(self):
        return self.read_table_and_convert('content.genre_film_work', pg_genre_film_work_to_dataclass)