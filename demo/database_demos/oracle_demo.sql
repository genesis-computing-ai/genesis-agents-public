-- Connect as system admin first
-- CONNECT system/my_password123@XEPDB1

-- Create schemas (users)
CREATE USER movies IDENTIFIED BY pwd;
CREATE USER tv IDENTIFIED BY pwd;
CREATE USER oracle_demo IDENTIFIED BY demo_password;

-- Grant permissions to schemas
GRANT CREATE SESSION, CREATE TABLE TO movies;
GRANT CREATE SESSION, CREATE TABLE TO tv;
GRANT UNLIMITED TABLESPACE TO movies;
GRANT UNLIMITED TABLESPACE TO tv;
GRANT CREATE SESSION TO oracle_demo;
GRANT UNLIMITED TABLESPACE TO oracle_demo;

-- Create Movies Schema Tables
CREATE TABLE movies.films (
    film_id NUMBER PRIMARY KEY,
    title VARCHAR2(100) NOT NULL,
    release_year NUMBER(4),
    director VARCHAR2(100),
    genre VARCHAR2(50),
    box_office_millions NUMBER(10,2)
);

CREATE TABLE movies.actors (
    actor_id NUMBER PRIMARY KEY,
    first_name VARCHAR2(50),
    last_name VARCHAR2(50),
    birth_date DATE,
    nationality VARCHAR2(50)
);

CREATE TABLE movies.film_actors (
    film_id NUMBER,
    actor_id NUMBER,
    role VARCHAR2(100),
    CONSTRAINT pk_film_actors PRIMARY KEY (film_id, actor_id),
    CONSTRAINT fk_film FOREIGN KEY (film_id) REFERENCES movies.films(film_id),
    CONSTRAINT fk_actor FOREIGN KEY (actor_id) REFERENCES movies.actors(actor_id)
);

-- Create TV Schema Tables
CREATE TABLE tv.shows (
    show_id NUMBER PRIMARY KEY,
    title VARCHAR2(100) NOT NULL,
    first_air_date DATE,
    network VARCHAR2(50),
    status VARCHAR2(20),
    total_seasons NUMBER
);

CREATE TABLE tv.episodes (
    episode_id NUMBER PRIMARY KEY,
    show_id NUMBER,
    season_number NUMBER,
    episode_number NUMBER,
    title VARCHAR2(100),
    air_date DATE,
    rating NUMBER(3,1),
    CONSTRAINT fk_show FOREIGN KEY (show_id) REFERENCES tv.shows(show_id)
);

-- Grant permissions to application user (oracle_demo) AFTER tables exist
GRANT SELECT, INSERT, UPDATE, DELETE ON movies.films TO oracle_demo;
GRANT SELECT, INSERT, UPDATE, DELETE ON movies.actors TO oracle_demo;
GRANT SELECT, INSERT, UPDATE, DELETE ON movies.film_actors TO oracle_demo;
GRANT SELECT, INSERT, UPDATE, DELETE ON tv.shows TO oracle_demo;
GRANT SELECT, INSERT, UPDATE, DELETE ON tv.episodes TO oracle_demo;
GRANT EXECUTE ON sys.dbms_metadata TO oracle_demo;

-- Sample Movies Data
INSERT INTO movies.films VALUES (1, 'The Shawshank Redemption', 1994, 'Frank Darabont', 'Drama', 58.80);
INSERT INTO movies.films VALUES (2, 'Inception', 2010, 'Christopher Nolan', 'Sci-Fi', 836.80);
INSERT INTO movies.films VALUES (3, 'Pulp Fiction', 1994, 'Quentin Tarantino', 'Crime', 213.90);
INSERT INTO movies.films VALUES (4, 'The Dark Knight', 2008, 'Christopher Nolan', 'Action', 1004.60);
INSERT INTO movies.films VALUES (5, 'Forrest Gump', 1994, 'Robert Zemeckis', 'Drama', 678.20);

INSERT INTO movies.actors VALUES (1, 'Morgan', 'Freeman', TO_DATE('1937-06-01', 'YYYY-MM-DD'), 'American');
INSERT INTO movies.actors VALUES (2, 'Leonardo', 'DiCaprio', TO_DATE('1974-11-11', 'YYYY-MM-DD'), 'American');
INSERT INTO movies.actors VALUES (3, 'John', 'Travolta', TO_DATE('1954-02-18', 'YYYY-MM-DD'), 'American');
INSERT INTO movies.actors VALUES (4, 'Christian', 'Bale', TO_DATE('1974-01-30', 'YYYY-MM-DD'), 'British');
INSERT INTO movies.actors VALUES (5, 'Tom', 'Hanks', TO_DATE('1956-07-09', 'YYYY-MM-DD'), 'American');

INSERT INTO movies.film_actors VALUES (1, 1, 'Red');
INSERT INTO movies.film_actors VALUES (2, 2, 'Cobb');
INSERT INTO movies.film_actors VALUES (3, 3, 'Vincent Vega');
INSERT INTO movies.film_actors VALUES (4, 4, 'Bruce Wayne');
INSERT INTO movies.film_actors VALUES (5, 5, 'Forrest Gump');

-- Sample TV Data
INSERT INTO tv.shows VALUES (1, 'Breaking Bad', TO_DATE('2008-01-20', 'YYYY-MM-DD'), 'AMC', 'Ended', 5);
INSERT INTO tv.shows VALUES (2, 'Stranger Things', TO_DATE('2016-07-15', 'YYYY-MM-DD'), 'Netflix', 'Running', 4);
INSERT INTO tv.shows VALUES (3, 'The Office', TO_DATE('2005-03-24', 'YYYY-MM-DD'), 'NBC', 'Ended', 9);
INSERT INTO tv.shows VALUES (4, 'Game of Thrones', TO_DATE('2011-04-17', 'YYYY-MM-DD'), 'HBO', 'Ended', 8);
INSERT INTO tv.shows VALUES (5, 'The Mandalorian', TO_DATE('2019-11-12', 'YYYY-MM-DD'), 'Disney+', 'Running', 3);

INSERT INTO tv.episodes VALUES (1, 1, 1, 1, 'Pilot', TO_DATE('2008-01-20', 'YYYY-MM-DD'), 8.9);
INSERT INTO tv.episodes VALUES (2, 2, 1, 1, 'Chapter One: The Vanishing of Will Byers', TO_DATE('2016-07-15', 'YYYY-MM-DD'), 8.7);
INSERT INTO tv.episodes VALUES (3, 3, 1, 1, 'Pilot', TO_DATE('2005-03-24', 'YYYY-MM-DD'), 7.8);
INSERT INTO tv.episodes VALUES (4, 4, 1, 1, 'Winter Is Coming', TO_DATE('2011-04-17', 'YYYY-MM-DD'), 9.1);
INSERT INTO tv.episodes VALUES (5, 5, 1, 1, 'Chapter 1: The Mandalorian', TO_DATE('2019-11-12', 'YYYY-MM-DD'), 8.8);

COMMIT;

-- Verify data
SELECT 'Movies Schema' as "Check", COUNT(*) as "Count" FROM movies.films
UNION ALL
SELECT 'Actors', COUNT(*) FROM movies.actors
UNION ALL
SELECT 'Film Actors', COUNT(*) FROM movies.film_actors
UNION ALL
SELECT 'TV Shows', COUNT(*) FROM tv.shows
UNION ALL
SELECT 'Episodes', COUNT(*) FROM tv.episodes;

CONNECT sys/my_password123@XEPDB1 AS SYSDBA;
GRANT EXECUTE ON sys.dbms_metadata TO oracle_demo;
