import sys
import os
import psycopg2
import contextlib
from dotenv import load_dotenv
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)),'../..'))
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)),'../../..'))
from sqlite_to_postgres.sqlite.sqlite_connector import SQLiteConnector, sqlite_connection_context
from sqlite_to_postgres.pg.pg_connector import PostgresConnector


def compare_tables(pg_reader, sqlite_reader):
    sqlite_generator = sqlite_reader()
    for pg_part in pg_reader():
        sqlite_part = next(sqlite_generator)

        if not pg_part or not sqlite_part:
            break

        assert len(pg_part) == len(sqlite_part)

        for i in range(len(pg_part)):
            pg_item = pg_part[i]
            sqlite_item = sqlite_part[i]

            if pg_item != sqlite_item:
                print(pg_item, '!=\n', sqlite_item)
            assert pg_item == sqlite_item


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

    page_size = 4000
    with contextlib.closing(psycopg2.connect(**dsn)) as conn:
        pg_connector = PostgresConnector(connection=conn, page_size=page_size)

        sqlite_db_path = os.path.join(os.path.dirname(__file__), '../../db.sqlite')
        with sqlite_connection_context(sqlite_db_path) as conn:
            sqlite_connector = SQLiteConnector(conn, page_size=page_size)

            comparation_mapping = {
                pg_connector.read_person: sqlite_connector.read_person,
                pg_connector.read_film_work: sqlite_connector.read_film_work,
                pg_connector.read_genre: sqlite_connector.read_genre,
                pg_connector.read_person_film_work: sqlite_connector.read_person_film_work,
                pg_connector.read_genre_film_work: sqlite_connector.read_genre_film_work,
            }

            for (pg_reader, sqlite_reader) in comparation_mapping.items():
                compare_tables(pg_reader, sqlite_reader)