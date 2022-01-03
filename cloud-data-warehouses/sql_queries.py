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
        artist varchar,
        auth varchar,
        firstName varchar,
        gender varchar,
        itemInSession int,
        lastName varchar,
        length numeric,
        level varchar,
        location varchar,
        method varchar,
        page varchar,
        registration numeric,
        sessionId int,
        song varchar,
        status int,
        ts timestamp,
        userAgent varchar,
        userId int
    )
"""

staging_songs_table_create = """
    CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs int,
        artist_id varchar,
        artist_latitude numeric,
        artist_longitude numeric,
        artist_location varchar,
        artist_name varchar,
        song_id varchar,
        title varchar,
        duration numeric,
        year int
    )
"""

songplay_table_create = """
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id int IDENTITY(0, 1) primary key,
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

staging_events_copy = f"""
    COPY staging_events FROM '{config["S3"]["LOG_DATA"]}'
    CREDENTIALS 'aws_iam_role={config["IAM_ROLE"]["ARN"]}'
    format as json '{config["S3"]["LOG_JSONPATH"]}'
    timeformat 'epochmillisecs'
    region 'us-west-2'
"""

staging_songs_copy = f"""
    COPY staging_songs FROM '{config["S3"]["SONG_DATA"]}'
    CREDENTIALS 'aws_iam_role={config["IAM_ROLE"]["ARN"]}'
    format as json 'auto'
    region 'us-west-2'
"""

staging_songs_copy_one_file = f"""
    COPY staging_songs FROM 's3://udacity-dend/song_data/A/A/A'
    CREDENTIALS 'aws_iam_role={config["IAM_ROLE"]["ARN"]}'
    format as json 'auto' region 'us-west-2'
"""

# FINAL TABLES

songplay_table_insert = """
"""

user_table_insert = """
    INSERT INTO
      users (
        user_id,
        first_name,
        last_name,
        gender,
        level
      )
    SELECT
      DISTINCT userid,
      firstname,
      lastname,
      gender,
      level
    FROM
      staging_events
    WHERE
      page = 'NextSong'
      AND userid NOT IN (SELECT DISTINCT user_id FROM users)
"""

song_table_insert = """
    INSERT INTO
      songs (
        song_id,
        title,
        artist_id,
        year,
        duration
      )
    SELECT
      DISTINCT song_id,
      title,
      artist_id,
      year,
      duration
    FROM
      staging_songs
    WHERE
      song_id NOT IN (SELECT DISTINCT song_id FROM songs)
"""

artist_table_insert = """
    INSERT INTO
      artists (
        artist_id,
        name,
        location,
        latitude,
        longitude
      )
    SELECT
      DISTINCT artist_id,
      artist_name,
      artist_location,
      artist_latitude,
      artist_longitude
    FROM
      staging_songs
    WHERE
      artist_id NOT IN (SELECT DISTINCT artist_id FROM artists)
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
# copy_table_queries = [staging_events_copy, staging_songs_copy]
copy_table_queries = [staging_events_copy, staging_songs_copy_one_file]
insert_table_queries = [
    # songplay_table_insert,
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    # time_table_insert,
]
