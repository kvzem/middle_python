import sqlite_connector
import pg_connector


film_works = sqlite_connector.read_film_work()
people = sqlite_connector.read_person()
genres = sqlite_connector.read_genre()
genre_film_work = sqlite_connector.read_genre_film_work()
person_film_work = sqlite_connector.read_person_film_work()


pg_connector.write_person(people)
pg_connector.write_film_work(film_works)
pg_connector.write_genre(genres)
pg_connector.write_genre_film_work(genre_film_work)
pg_connector.write_person_film_work(person_film_work)
