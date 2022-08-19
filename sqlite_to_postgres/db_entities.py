from dataclasses import dataclass, field
import uuid
from datetime import datetime


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
    created: datetime


@dataclass
class PersonFilmWork(HasUUID):
    film_work_id: uuid.UUID
    person_id: uuid.UUID
    role: str
    created: str

