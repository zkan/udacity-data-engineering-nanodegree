# Song Play Analysis

## Project Overview

In this project, we aim to create a star schema optimized for queries on songs play analysis. We first build an ETL pipeline that extracts data from S3, stages them in Redshift, and transforms them into a set of dimensional tables for further analysis.

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
| `dwh.cfg` | A configuration file that contains credentials to connect to Redshift |
| `create_table.py` | A Python script that drops and creates fact and dimension tables for the star schema in Redshift |
| `etl.py` | A Python script that processes data and loads them into tables in Redshift |
| `sql_queries.py` | A Python script that contains all SQL queryes and is imported into other files |
| `pyproject.toml` | A file that contains Python package dependencies managed by Poetry for this code repository |

## Instruction on Running the Python Scripts

This project uses the [Poetry](https://python-poetry.org/) for managing Python dependencies.
Kindly go to the website and install it first. After that, please follow the steps below.

1. Install the dependencies.

    ```bash
    poetry install
    ```

1. Initialize all of the tables.

    ```bash
    poetry run python create_tables.py
    ```

    We can use the command above to refresh the database as well.

1. Then we perform the ETL process.

    ```bash
    poetry run python etl.py
    ```