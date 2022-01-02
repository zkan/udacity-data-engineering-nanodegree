import configparser


# CONFIG
config = configparser.ConfigParser()
config.read("dwh.cfg")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create = """
    CREATE TABLE IF NOT EXISTS staging_events (
    )
"""

staging_songs_table_create = """
    CREATE TABLE IF NOT EXISTS staging_songs (
    )
"""

songplay_table_create = """
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id serial primary key,
        start_time timestamp not null,
        user_id int not null,
        level varchar,
        song_id varchar,
        artist_id varchar,
        session_id int,
        location varchar,
        user_agent varchar
    )
"""

user_table_create = """
    CREATE TABLE IF NOT EXISTS users (
        user_id int primary key,
        first_name varchar not null,
        last_name varchar not null,
        gender varchar,
        level varchar
    )
"""

song_table_create = """
    CREATE TABLE IF NOT EXISTS songs (
        song_id varchar primary key,
        title varchar not null,
        artist_id varchar not null,
        year int,
        duration numeric
    )
"""

artist_table_create = """
    CREATE TABLE IF NOT EXISTS artists (
        artist_id varchar primary key,
        name varchar not null,
        location varchar,
        latitude numeric,
        longitude numeric
    )
"""

time_table_create = """
    CREATE TABLE IF NOT EXISTS time (
        start_time timestamp primary key,
        hour int,
        day int,
        week int,
        month int,
        year int,
        weekday int
    )
"""

# STAGING TABLES

staging_events_copy = (
    """
"""
).format()

staging_songs_copy = (
    """
"""
).format()

# FINAL TABLES

songplay_table_insert = """
"""

user_table_insert = """
"""

song_table_insert = """
"""

artist_table_insert = """
"""

time_table_insert = """
"""

# QUERY LISTS

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    songplay_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create,
]
drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop,
]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [
    songplay_table_insert,
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert,
]
