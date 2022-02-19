class SqlQueries:
    songplay_table_insert = """
        INSERT INTO
            songplays (
                playid,
                start_time,
                userid,
                level,
                songid,
                artistid,
                sessionid,
                location,
                user_agent
            )
        SELECT
            md5(events.sessionid || events.start_time) songplay_id,
            events.start_time,
            events.userid,
            events.level,
            songs.song_id,
            songs.artist_id,
            events.sessionid,
            events.location,
            events.useragent
        FROM (
            SELECT
                TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time,
                *
            FROM staging_events
            WHERE page='NextSong'
        ) events
        LEFT JOIN
            staging_songs songs
        ON
            events.song = songs.title
            AND events.artist = songs.artist_name
            AND events.length = songs.duration
        WHERE
            TIMESTAMP 'epoch' + ts/1000 * interval '1 second' NOT IN (SELECT DISTINCT start_time FROM songplays)
    """

    user_table_insert = """
        INSERT INTO
            users (
                userid,
                first_name,
                last_name,
                gender,
                level
            )
        SELECT
            distinct userid,
            firstname,
            lastname,
            gender,
            level
        FROM staging_events
        WHERE
            page='NextSong'
            AND userid NOT IN (SELECT DISTINCT userid FROM users)
    """

    song_table_insert = """
        INSERT INTO
            songs (
                songid,
                title,
                artistid,
                year,
                duration
            )
        SELECT
            distinct song_id,
            title,
            artist_id,
            year,
            duration
        FROM staging_songs
        WHERE
            song_id NOT IN (SELECT DISTINCT songid FROM songs)
    """

    artist_table_insert = """
        INSERT INTO
            artists (
                artistid,
                name,
                location,
                latitude,
                longitude
            )
        SELECT
            distinct artist_id,
            artist_name,
            artist_location,
            artist_latitude,
            artist_longitude
        FROM staging_songs
        WHERE
            artist_id NOT IN (SELECT DISTINCT artistid FROM artists)
    """

    time_table_insert = """
        INSERT INTO
            time (
                start_time,
                hour,
                day,
                week,
                month,
                year,
                weekday
            )
        SELECT
            start_time,
            extract(hour from start_time),
            extract(day from start_time),
            extract(week from start_time),
            extract(month from start_time),
            extract(year from start_time),
            extract(dayofweek from start_time)
        FROM songplays
        WHERE
            start_time NOT IN (SELECT DISTINCT start_time FROM time)
    """
