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
CREATE INDEX film_work_title_idx ON content.film_work(title);

CREATE TABLE IF NOT EXISTS content.person (
    id uuid PRIMARY KEY,
    full_name TEXT NOT NULL,
    created timestamp with time zone,
    modified timestamp with time zone
);

CREATE INDEX person_full_name_idx ON content.person(full_name);

CREATE TABLE IF NOT EXISTS content.person_film_work (
    id uuid PRIMARY KEY,
    film_work_id uuid NOT NULL,
    person_id uuid NOT NULL,
    role TEXT NOT NULL,
    created timestamp with time zone,
    FOREIGN KEY (film_work_id) REFERENCES content.film_work(id),
    FOREIGN KEY (person_id) REFERENCES content.person(id)
);

ALTER TABLE content.person_film_work ADD CONSTRAINT unq_person_film_role_constraint UNIQUE (person_id, film_work_id, role);

CREATE TABLE IF NOT EXISTS content.genre (
    id uuid PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT NOT NULL,
    created timestamp with time zone,
    modified timestamp with time zone
);

CREATE INDEX genre_name ON content.genre(name);

CREATE TABLE IF NOT EXISTS content.genre_film_work (
    id uuid PRIMARY KEY,
    genre_id uuid NOT NULL,
    film_work_id uuid NOT NULL,
    created timestamp with time zone,
    FOREIGN KEY (genre_id) REFERENCES content.genre(id),
    FOREIGN KEY (film_work_id) REFERENCES content.film_work(id)
);

ALTER TABLE content.genre_film_work ADD CONSTRAINT unq_genre_film_work_constraint UNIQUE (film_work_id, genre_id);