from db_entities import Person, FilmWork, Genre, PersonFilmWork, GenreFilmWork
from datetime import datetime


def chop_date_time_str_until_dot(s):
    if '.' in s:
        index = s.index('.')
        return s[:index]
    return s


def sqlite_person_to_dataclass(sqlite_row):
    return Person(id=sqlite_row["id"],
                  full_name=sqlite_row["full_name"],
                  created=chop_date_time_str_until_dot(sqlite_row["created_at"]),
                  modified=chop_date_time_str_until_dot(sqlite_row["updated_at"]))


def sqlite_film_work_to_dataclass(sqlite_row):
    return FilmWork(id=sqlite_row["id"],
                    title=sqlite_row["title"],
                    description=sqlite_row["description"],
                    creation_date=sqlite_row["creation_date"],
                    rating=sqlite_row["rating"],
                    type=sqlite_row["type"],
                    created=chop_date_time_str_until_dot(sqlite_row["created_at"]),
                    modified=chop_date_time_str_until_dot(sqlite_row["updated_at"]))


def sqlite_genre_to_dataclass(sqlite_row):
    return Genre(id=sqlite_row["id"],
                 name=sqlite_row["name"],
                 description=sqlite_row["description"] if sqlite_row["description"] else "",
                 created=chop_date_time_str_until_dot(sqlite_row["created_at"]),
                 modified=chop_date_time_str_until_dot(sqlite_row["updated_at"]))


def sqlite_genre_film_work_to_dataclass(sqlite_row):
    return GenreFilmWork(id=sqlite_row["id"],
                         film_work_id=sqlite_row["film_work_id"],
                         genre_id=sqlite_row["genre_id"],
                         created=chop_date_time_str_until_dot(sqlite_row["created_at"]))


def sqlite_person_film_work_to_dataclass(sqlite_row):
    return PersonFilmWork(id=sqlite_row["id"],
                          film_work_id=sqlite_row["film_work_id"],
                          person_id=sqlite_row["person_id"],
                          role=sqlite_row["role"],
                          created=chop_date_time_str_until_dot(sqlite_row["created_at"]))
