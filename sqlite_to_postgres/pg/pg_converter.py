from datetime import datetime
from db_entities import Person, FilmWork, Genre, PersonFilmWork, GenreFilmWork


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
