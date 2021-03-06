from datetime import timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils import timezone

from operators import (
    StageToRedshiftOperator,
    LoadFactOperator,
    LoadDimensionOperator,
    DataQualityOperator,
)
from helpers import SqlQueries


default_args = {
    "owner": "Kan Ouivirach",
    "start_date": timezone.datetime(2022, 2, 1),
    "depends_on_past": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}
with DAG(
    "song_play_etl_pipeline",
    default_args=default_args,
    description="Load and transform data in Redshift with Airflow",
    schedule_interval="0 * * * *",
    catchup=False,
) as dag:

    start_operator = DummyOperator(task_id="Begin_execution")

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id="Stage_events",
        redshift_conn_id="redshift",
        iam_role="arn:aws:iam::573529480358:role/myRedshiftRole",
        table="staging_events",
        s3_bucket="udacity-dend",
        s3_key="log_data",
        json_path="log_json_path.json",
        time_format="epochmillisecs",
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id="Stage_songs",
        redshift_conn_id="redshift",
        iam_role="arn:aws:iam::573529480358:role/myRedshiftRole",
        table="staging_songs",
        s3_bucket="udacity-dend",
        s3_key="song_data",
    )

    load_songplays_table = LoadFactOperator(
        task_id="Load_songplays_fact_table",
        redshift_conn_id="redshift",
        insert_sql=SqlQueries.songplay_table_insert,
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id="Load_user_dim_table",
        redshift_conn_id="redshift",
        table="users",
        insert_sql=SqlQueries.user_table_insert,
        truncate=True,
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id="Load_song_dim_table",
        redshift_conn_id="redshift",
        table="songs",
        insert_sql=SqlQueries.song_table_insert,
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id="Load_artist_dim_table",
        redshift_conn_id="redshift",
        table="artists",
        insert_sql=SqlQueries.artist_table_insert,
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id="Load_time_dim_table",
        redshift_conn_id="redshift",
        table="time",
        insert_sql=SqlQueries.time_table_insert,
    )

    run_quality_checks = DataQualityOperator(
        task_id="Run_data_quality_checks",
        redshift_conn_id="redshift",
        checks=[
            {
                "test_case": "Column playid in table songplays should not have NULL values",
                "test_sql": "SELECT COUNT(playid) FROM songplays WHERE playid IS NULL",
                "expected_result": 0,
                "comparison": "=",
            },
            {
                "test_case": "Table songs should have records",
                "test_sql": "SELECT COUNT(*) FROM songs",
                "expected_result": 0,
                "comparison": ">",
            },
            {
                "test_case": "Column duration should have value less than 100000",
                "test_sql": "SELECT MAX(duration) FROM songs",
                "expected_result": 100000,
                "comparison": "<",
            },
        ],
    )

    end_operator = DummyOperator(task_id="Stop_execution")

    (
        start_operator
        >> [stage_events_to_redshift, stage_songs_to_redshift]
        >> load_songplays_table
        >> [
            load_user_dimension_table,
            load_song_dimension_table,
            load_artist_dimension_table,
            load_time_dimension_table,
        ]
        >> run_quality_checks
        >> end_operator
    )
