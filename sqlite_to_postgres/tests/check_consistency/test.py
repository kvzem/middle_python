import sys, os
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)),'../..'))
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)),'../../..'))
import sqlite_to_postgres.sqlite_connector as sqlite
import sqlite_to_postgres.pg_connector as pg


sqlite.db_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), '../..', 'db.sqlite')


def compare_tables(pg_table, sqlite_table):
    assert len(pg_table) == len(sqlite_table)

    for i in range(len(pg_table)):
        pg_item = pg_table[i]
        sqlite_item = sqlite_table[i]

        if pg_item != sqlite_item:
            print(pg_item, '!=', sqlite_item)
        assert pg_item == sqlite_item


def test_person_entries():
    pg_data = pg.read_person()
    sqlite_data = sqlite.read_person()

    compare_tables(pg_data, sqlite_data)


def test_film_work_entries():
    pg_data = pg.read_film_work()
    sqlite_data = sqlite.read_film_work()

    compare_tables(pg_data, sqlite_data)


def test_genre_entities():
    pg_data = pg.read_genre()
    sqlite_data = sqlite.read_genre()

    compare_tables(pg_data, sqlite_data)


def test_person_film_work_entities():
    pg_data = pg.read_person_film_work()
    sqlite_data = sqlite.read_person_film_work()

    compare_tables(pg_data, sqlite_data)


def test_genre_film_work_entities():
    pg_data = pg.read_genre_film_work()
    sqlite_data = sqlite.read_genre_film_work()

    compare_tables(pg_data, sqlite_data)


test_person_entries()
test_film_work_entries()
test_genre_entities()
test_person_film_work_entities()
test_genre_film_work_entities()