import psycopg2


def test_it_should_be_seventy_one_rows_in_song_where_artist_id_is_not_null():
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=sparkifydb user=student password=student"
    )
    cur = conn.cursor()

    select = """
        SELECT * FROM songs WHERE artist_id IS NOT NULL;
    """
    cur.execute(select)
    results = cur.fetchall()

    assert len(results) == 71

    print("✅ There are 71 rows where artist ID is not null")

    conn.close()


def test_it_should_be_one_row_where_both_song_id_and_artist_id_are_not_null():
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=sparkifydb user=student password=student"
    )
    cur = conn.cursor()

    select = """
        SELECT
            song_id,
            artist_id
        FROM songplays
        WHERE song_id IS NOT NULL AND artist_id IS NOT NULL;
    """
    cur.execute(select)
    results = cur.fetchall()

    assert len(results) == 1

    song_id, artist_id = results[0]
    assert song_id == "SOZCTXZ12AB0182364"
    assert artist_id == "AR5KOSW1187FB35FF4"

    print("✅ There is 1 row with values for value containing ID for both song ID and artist ID")

    conn.close()
