from dataclasses import dataclass, field
import uuid


@dataclass
class TimeStamped:
    created: str
    modified: str


@dataclass
class HasUUID:
    id: uuid.UUID


@dataclass
class Person(TimeStamped, HasUUID):
    full_name: str


@dataclass
class FilmWork(TimeStamped, HasUUID):
    title: str
    description: str
    creation_date: str
    file_path: str
    rating: float
    type: str


@dataclass
class Genre(TimeStamped, HasUUID):
    name: str
    description: str = field(default="")


@dataclass
class GenreFilmWork(HasUUID):
    film_work_id: uuid.UUID
    genre_id: uuid.UUID
    created: str


@dataclass
class PersonFilmWork(HasUUID):
    film_work_id: uuid.UUID
    person_id: uuid.UUID
    role: str
    created: str


def sqlite_person_to_dataclass(sqlite_row):
    return Person(id=sqlite_row["id"],
                  full_name=sqlite_row["full_name"],
                  created=sqlite_row["created_at"],
                  modified=sqlite_row["updated_at"])


def sqlite_film_work_to_dataclass(sqlite_row):
    return FilmWork(id=sqlite_row["id"],
                    title=sqlite_row["title"],
                    description=sqlite_row["description"],
                    creation_date=sqlite_row["creation_date"],
                    file_path=sqlite_row["file_path"],
                    rating=sqlite_row["rating"],
                    type=sqlite_row["type"],
                    created=sqlite_row["created_at"],
                    modified=sqlite_row["updated_at"])


def sqlite_genre_to_dataclass(sqlite_row):
    return Genre(id=sqlite_row["id"],
                 name=sqlite_row["name"],
                 description=sqlite_row["description"] if sqlite_row["description"] else "",
                 created=sqlite_row["created_at"],
                 modified=sqlite_row["updated_at"])


def sqlite_genre_film_work_to_dataclass(sqlite_row):
    return GenreFilmWork(id=sqlite_row["id"],
                         film_work_id=sqlite_row["film_work_id"],
                         genre_id=sqlite_row["genre_id"],
                         created=sqlite_row["created_at"])


def sqlite_person_film_work_to_dataclass(sqlite_row):
    return PersonFilmWork(id=sqlite_row["id"],
                          film_work_id=sqlite_row["film_work_id"],
                          person_id=sqlite_row["person_id"],
                          role=sqlite_row["role"],
                          created=sqlite_row["created_at"])
