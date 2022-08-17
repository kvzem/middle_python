import contextlib
import psycopg2
from psycopg2.extras import execute_batch


dsn = {
    'dbname': 'movies_database',
    'user': 'app',
    'password': '123qwe',
    'host': 'localhost',
    'port': 5435,
    'options': '-c search_path=content',
}
PAGE_SIZE = 5000


def write_person(people):
    with contextlib.closing(psycopg2.connect(**dsn)) as conn, conn.cursor() as cur:
        query = """INSERT INTO person (id, full_name, created, modified)
                   VALUES (%s, %s, %s, %s) ON CONFLICT do nothing"""
        data = [(item.id, item.full_name, item.created, item.modified) for item in people]
        execute_batch(cur, query, data, page_size=PAGE_SIZE)
        conn.commit()


def write_film_work(film_works):
    with contextlib.closing(psycopg2.connect(**dsn)) as conn, conn.cursor() as cur:
        query = """INSERT INTO film_work (id, title, description, creation_date, rating, type, created, modified)
                                  VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT do nothing"""
        data = [(item.id, item.title, item.description, item.creation_date,
                 item.rating, item.type, item.created, item.modified) for item in film_works]
        execute_batch(cur, query, data, page_size=PAGE_SIZE)
        conn.commit()


def write_genre(genres):
    with contextlib.closing(psycopg2.connect(**dsn)) as conn, conn.cursor() as cur:
        query = """INSERT INTO genre (id, name, description, created, modified)
                              VALUES (%s, %s, %s, %s, %s) ON CONFLICT do nothing"""
        data = [(item.id, item.name, item.description, item.created, item.modified) for item in genres]
        execute_batch(cur, query, data, page_size=PAGE_SIZE)
        conn.commit()


def write_genre_film_work(genre_film_work):
    with contextlib.closing(psycopg2.connect(**dsn)) as conn, conn.cursor() as cur:
        query = """INSERT INTO genre_film_work (id, genre_id, film_work_id, created)
                                        VALUES (%s, %s, %s, %s) ON CONFLICT do nothing"""
        data = [(item.id, item.genre_id, item.film_work_id, item.created) for item in genre_film_work]
        execute_batch(cur, query, data, page_size=PAGE_SIZE)
        conn.commit()


def write_person_film_work(genre_film_work):
    with contextlib.closing(psycopg2.connect(**dsn)) as conn, conn.cursor() as cur:
        query = """INSERT INTO person_film_work (id, person_id, film_work_id, role, created)
                                         VALUES (%s, %s, %s, %s, %s) ON CONFLICT do nothing"""
        data = [(item.id, item.person_id, item.film_work_id, item.role, item.created) for item in genre_film_work]
        execute_batch(cur, query, data, page_size=PAGE_SIZE)
        conn.commit()


def get_count(table):
    with contextlib.closing(psycopg2.connect(**dsn)) as conn, conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table}".format(table=table))
        result = cur.fetchone()[0]
        return result


def get_person_count():
    return get_count("person")


def get_film_work_count():
    return get_count("film_work")


def get_genre_count():
    return get_count("genre")


def get_person_film_work_count():
    return get_count("person_film_work")


def get_genre_film_work_count():
    return get_count("genre_film_work")
