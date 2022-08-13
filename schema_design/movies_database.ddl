CREATE SCHEMA IF NOT EXISTS content;

CREATE TABLE IF NOT EXISTS content.film_work (
    id uuid PRIMARY KEY,
    title TEXT NOT NULL,
    description TEXT,
    creation_date DATE,
    rating FLOAT,
    type TEXT NOT NULL,
    created timestamp with time zone,
    modified timestamp with time zone
);

CREATE INDEX film_work_creation_date_idx ON content.film_work(creation_date);

CREATE TABLE IF NOT EXISTS content.person (
    id uuid PRIMARY KEY,
    full_name TEXT NOT NULL,
    created timestamp with time zone,
    modified timestamp with time zone
);

CREATE TABLE IF NOT EXISTS content.person_film_work (
    id uuid PRIMARY KEY,
    film_work_id uuid NOT NULL,
    person_id uuid NOT NULL,
    role TEXT NOT NULL,
    created timestamp with time zone,
    FOREIGN KEY (film_work_id) REFERENCES film_work(film_work_id),
    FOREIGN KEY (person_id) REFERENCES person(person_id)
);

-- Устанавливаем расширения для генерации UUID
CREATE EXTENSION "uuid-ossp";

CREATE TABLE IF NOT EXISTS content.genre (
    id uuid PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT NOT NULL,
    created timestamp with time zone,
    modified timestamp with time zone
);

CREATE TABLE IF NOT EXISTS content.genre_film_work (
    id uuid PRIMARY KEY,
    genre_id uuid NOT NULL,
    film_work_id uuid NOT NULL,
    created timestamp with time zone,
    FOREIGN KEY (genre_id) REFERENCES content.genre(id),
    FOREIGN KEY (film_work_id) REFERENCES content.film_work(id)
);