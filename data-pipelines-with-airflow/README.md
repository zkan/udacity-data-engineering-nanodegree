# Song Play Analysis Pipeline

## Project Overview

In this project, we build an automated ELT data pipeline with Airflow that aims to create a star schema optimized for queries on songs play analysis. We first extract data from S3, stage them in Redshift, and transform them into a set of dimensional tables for further analysis.

## Schema for Song Play Analysis

Here we have the fact table:

* `songplays` - records in event data associated with song plays

and the dimension tables:

* `users` - users in the app
* `songs` - songs in the database
* `time` - timestamps of records in songplays
* `artists` - artists in database

## Files and What They Do

| Name | Description |
| - | - |
| `mnt/dags/song_play_etl_pipeline.py` | A DAG file that processes data and loads them into tables in Redshift |
| `mnt/helpers/sql_queries.py` | A Python script that SQL statements used to run ETL |
| `mnt/operators/data_quality.py` | A Python script that implement `DataQualityOperator` to run data quality check if certain column contains NULL values |
| `mnt/operators/load_dimension.py` | A Python script that implement `LoadDimensionOperator` to load data from staging tables to dimension table |
| `mnt/operators/load_fact.py` | A Python script that implement `LoadFactOperator` to load data from staging tables to fact table |
| `mnt/operators/stage_redshift.py` | A Python script that implement `StageToRedshiftOperator` to load data from S3 to Redshift |
| `mnt/create_tables.sql` | A SQL file that creates all tables we used in this project |
| `docker-compose.yaml` | A Docker Compose file that runs the Airflow |
| `pyproject.toml` | A file that contains Python package dependencies managed by Poetry for this code repository |
| `README.md` | README file that provides discussion on this project |
| `setup.cfg` | A configuration file for Flake8 |

## Starting Airflow on Local

```
mkdir -p mnt/logs
echo -e "AIRFLOW_UID=$(id -u)" > .env
docker-compose up
```
