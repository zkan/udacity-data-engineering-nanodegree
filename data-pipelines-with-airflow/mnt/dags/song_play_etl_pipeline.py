import os
from datetime import timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.utils import timezone

from operators import (
    StageToRedshiftOperator,
    LoadFactOperator,
    LoadDimensionOperator,
    DataQualityOperator,
)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

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

    stage_events_to_redshift = StageToRedshiftOperator(task_id="Stage_events")

    stage_songs_to_redshift = StageToRedshiftOperator(task_id="Stage_songs")

    load_songplays_table = LoadFactOperator(task_id="Load_songplays_fact_table")

    load_user_dimension_table = LoadDimensionOperator(
        task_id="Load_user_dim_table"
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id="Load_song_dim_table"
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id="Load_artist_dim_table"
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id="Load_time_dim_table"
    )

    run_quality_checks = DataQualityOperator(task_id="Run_data_quality_checks")

    end_operator = DummyOperator(task_id="Stop_execution")

    start_operator >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks >> end_operator
