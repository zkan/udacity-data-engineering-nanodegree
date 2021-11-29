import psycopg2
import pytest


@pytest.fixture
def potgres_connection():
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=sparkifydb user=student password=student"
    )
    cur = conn.cursor()

    yield conn, cur

    conn.close()


def test_it_should_be_seventy_one_rows_in_song_where_artist_id_is_not_null(
    potgres_connection,
):
    conn, cur = potgres_connection

    select = """
        SELECT * FROM songs WHERE artist_id IS NOT NULL;
    """
    cur.execute(select)
    results = cur.fetchall()

    assert len(results) == 71


def test_it_should_be_one_row_where_both_song_id_and_artist_id_are_not_null(
    potgres_connection,
):
    conn, cur = potgres_connection

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
