import sqlite_to_postgres.sqlite_connector as sqlite
import sqlite_to_postgres.pg_connector as pg


def test_counts():
    sqlite_person_count = sqlite.get_person_count()
    pg_person_count = pg.get_person_count()
    assert sqlite_person_count == pg_person_count

    sqlite_film_work_count = sqlite.get_film_work_count()
    pg_film_work_count = pg.get_film_work_count()
    assert sqlite_film_work_count == pg_film_work_count

    sqlite_genre_count = sqlite.get_genre_count()
    pg_genre_count = pg.get_genre_count()
    assert sqlite_genre_count == pg_genre_count

    sqlite_person_film_work_count = sqlite.get_person_film_work_count()
    pg_person_film_work_count = pg.get_person_film_work_count()
    assert sqlite_person_film_work_count == pg_person_film_work_count

    sqlite_genre_film_work_count = sqlite.get_genre_film_work_count()
    pg_genre_film_work_count = pg.get_genre_film_work_count()
    assert sqlite_genre_film_work_count == pg_genre_film_work_count


test_counts()
