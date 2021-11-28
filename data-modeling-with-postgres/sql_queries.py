# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = """
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id int,
        start_time int,
        user_id int,
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
        user_id varchar,
        first_name varchar,
        last_name varchar,
        gender varchar,
        level varchar
    )
"""

song_table_create = """
    CREATE TABLE IF NOT EXISTS songs (
        song_id varchar,
        title varchar,
        artist_id varchar,
        year int,
        duration numeric
    )
"""

artist_table_create = """
    CREATE TABLE IF NOT EXISTS artists (
        artist_id varchar,
        name varchar,
        location varchar,
        latitude numeric,
        longitude numeric
    )
"""

time_table_create = """
    CREATE TABLE IF NOT EXISTS time (
        start_time int,
        hour int,
        day int,
        week int,
        month int,
        year int,
        weekday int
    )
"""

# INSERT RECORDS

songplay_table_insert = """
    INSERT INTO songplays (
        songplay_id,
        start_time,
        user_id,
        level,
        song_id,
        artist_id,
        session_id,
        location,
        user_agent
    ) VALUES (%d, %d, %d, %s, %s, %s, %d, %s, %s)
"""

user_table_insert = """
"""

song_table_insert = """
"""

artist_table_insert = """
"""


time_table_insert = """
"""

# FIND SONGS

song_select = """
"""

# QUERY LISTS

create_table_queries = [
    songplay_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create,
]
drop_table_queries = [
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop,
]
