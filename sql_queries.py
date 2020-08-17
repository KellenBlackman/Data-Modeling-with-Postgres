# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id bigint PRIMARY KEY,
        start_time bigint,
        user_id integer NOT NULL,
        song_id text NULL,
        artist_id text NULL,
        session_id integer NOT NULL,
        user_agent text,
        level text,
        location text)
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id integer PRIMARY KEY,
        first_name text,
        last_name text,
        gender varchar(10),
        level varchar(10) NOT NULL)
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id text PRIMARY KEY,
        title text NOT NULL,
        artist_id text,
        year integer NOT NULL,
        duration numeric)
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id text PRIMARY KEY,
        name text NOT NULL,
        location text,
        latitude text,
        longitude text)
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time bigint PRIMARY KEY,
        hour integer NOT NULL,
        day integer NOT NULL,
        week integer NOT NULL,
        month integer NOT NULL,
        year integer NOT NULL,
        weekday integer NOT NULL
        )
""")

# INSERT RECORDS

songplay_table_insert = ("""
INSERT INTO songplays (songplay_id, start_time, user_id, song_id, artist_id, session_id, user_agent, level, location)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level) 
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (user_id)
DO UPDATE SET level = excluded.level, first_name = excluded.first_name, last_name = excluded.last_name, gender = excluded.gender
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration) 
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (song_id)
DO NOTHING
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude) 
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (artist_id)
DO NOTHING
""")


time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday) 
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (start_time)
DO NOTHING
""")

# FIND SONGS

song_select = ("""
select s.song_id, s.artist_id
from songs s
join artists a on a.artist_id = s.artist_id
where s.title = %s and a.name = %s and s.duration = %s
""")

# QUERY LISTS

create_table_queries = [user_table_create, artist_table_create, song_table_create, songplay_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]