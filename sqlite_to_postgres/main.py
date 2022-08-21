from dotenv import load_dotenv
import os
import contextlib
import psycopg2
from pg.pg_connector import PostgresConnector
from sqlite.sqlite_connector import sqlite_connection_context, SQLiteConnector


if __name__ == '__main__':
    load_dotenv()
    dsn = {
        'dbname': os.environ.get('DB_NAME'),
        'user': os.environ.get('DB_USER'),
        'password': os.environ.get('DB_PASSWORD'),
        'host': os.environ.get('DB_HOST', '127.0.0.1'),
        'port': os.environ.get('DB_PORT', 5432),
        'options': '-c search_path=content',
    }
    with contextlib.closing(psycopg2.connect(**dsn)) as conn:
        pg_connector = PostgresConnector(connection=conn, page_size=1000)

        sqlite_db_path = os.path.join(os.path.dirname(__file__), 'db.sqlite')
        with sqlite_connection_context(sqlite_db_path) as conn:
            sqlite_connector = SQLiteConnector(conn, page_size=1000)

            generators_mapping = {
                sqlite_connector.read_film_work: pg_connector.write_film_work,
                sqlite_connector.read_person: pg_connector.write_person,
                sqlite_connector.read_genre: pg_connector.write_genre,
                sqlite_connector.read_person_film_work: pg_connector.write_person_film_work,
                sqlite_connector.read_genre_film_work: pg_connector.write_genre_film_work,
            }

            for (sqlite_reader, pg_writer) in generators_mapping.items():
                for part in sqlite_reader():
                    pg_writer(part)
