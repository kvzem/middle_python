import contextlib
import psycopg2
import os
from psycopg2.extras import execute_batch
from db_entities import Person, FilmWork, Genre, PersonFilmWork, GenreFilmWork
from datetime import datetime
from dotenv import load_dotenv


load_dotenv()
dsn = {
    'dbname': os.environ.get('DB_NAME'),
    'user': os.environ.get('DB_USER'),
    'password': os.environ.get('DB_PASSWORD'),
    'host': os.environ.get('DB_HOST', '127.0.0.1'),
    'port': os.environ.get('DB_PORT', 5432),
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


def write_genre(genres):
    with contextlib.closing(psycopg2.connect(**dsn)) as conn, conn.cursor() as cur:
        query = """INSERT INTO genre (id, name, description, created, modified)
                              VALUES (%s, %s, %s, %s, %s) ON CONFLICT do nothing"""
        data = [(item.id, item.name, item.description, item.created, item.modified) for item in genres]
        execute_batch(cur, query, data, page_size=PAGE_SIZE)


def write_genre_film_work(genre_film_work):
    with contextlib.closing(psycopg2.connect(**dsn)) as conn, conn.cursor() as cur:
        query = """INSERT INTO genre_film_work (id, genre_id, film_work_id, created)
                                        VALUES (%s, %s, %s, %s) ON CONFLICT do nothing"""
        data = [(item.id, item.genre_id, item.film_work_id, item.created) for item in genre_film_work]
        execute_batch(cur, query, data, page_size=PAGE_SIZE)


def write_person_film_work(genre_film_work):
    with contextlib.closing(psycopg2.connect(**dsn)) as conn, conn.cursor() as cur:
        query = """INSERT INTO person_film_work (id, person_id, film_work_id, role, created)
                                         VALUES (%s, %s, %s, %s, %s) ON CONFLICT do nothing"""
        data = [(item.id, item.person_id, item.film_work_id, item.role, item.created) for item in genre_film_work]
        execute_batch(cur, query, data, page_size=PAGE_SIZE)


def convert_datetime(dt):
    if type(dt) is datetime:
        return datetime.strftime(dt, '%Y-%m-%d %H:%M:%S')
    return dt


def pg_person_to_dataclass(pg_person):
    return Person(id=pg_person[0],
                  full_name=pg_person[1],
                  created=convert_datetime(pg_person[2]),
                  modified=convert_datetime(pg_person[3]))


def pg_film_work_to_dataclass(pg_film_work):
    return FilmWork(id = pg_film_work[0],
                    title=pg_film_work[1],
                    description=pg_film_work[2],
                    creation_date=convert_datetime(pg_film_work[3]),
                    rating=pg_film_work[4],
                    type=pg_film_work[5],
                    created=convert_datetime(pg_film_work[6]),
                    modified=convert_datetime(pg_film_work[7]))


def pg_genre_to_dataclass(pg_genre):
    return Genre(id=pg_genre[0],
                 name=pg_genre[1],
                 description=pg_genre[2],
                 created=convert_datetime(pg_genre[3]),
                 modified=convert_datetime(pg_genre[4]))


def pg_person_film_work_to_dataclass(pg_person_film_work):
    return PersonFilmWork(id=pg_person_film_work[0],
                          film_work_id=pg_person_film_work[1],
                          person_id=pg_person_film_work[2],
                          role=pg_person_film_work[3],
                          created=convert_datetime(pg_person_film_work[4]))


def pg_genre_film_work_to_dataclass(pg_genre_film_work):
    return GenreFilmWork(id=pg_genre_film_work[0],
                         genre_id=pg_genre_film_work[1],
                         film_work_id=pg_genre_film_work[2],
                         created=convert_datetime(pg_genre_film_work[3]))


def read_table_and_convert(table, convertion_func):
    with contextlib.closing(psycopg2.connect(**dsn)) as conn, conn.cursor() as cur:
        cur.execute(f"SELECT * FROM {table} ORDER BY id".format(table=table))
        sql_rows = cur.fetchall()
        result = [convertion_func(row) for row in sql_rows]
        return result


def read_person():
    return read_table_and_convert('content.person', pg_person_to_dataclass)


def read_film_work():
    return read_table_and_convert('content.film_work', pg_film_work_to_dataclass)


def read_genre():
    return read_table_and_convert('content.genre', pg_genre_to_dataclass)


def read_person_film_work():
    return read_table_and_convert('content.person_film_work', pg_person_film_work_to_dataclass)


def read_genre_film_work():
    return read_table_and_convert('content.genre_film_work', pg_genre_film_work_to_dataclass)