# Song Play Analysis

I define the fact and dimension tables for a star schema for a song play
analysis, and write an ETL pipeline to transform data from files and load
into the tables in Postgres using Python and SQL.

In this project, we're interested in understanding what songs users
are listening to.

## How to Run this Project

Before we start running the ETL process, we'll need to run the Postgres
database first. Here I set up the Docker Compose for it. To start the
Postgres, run:

```bash
docker-compose up -d
```

This project uses the [Poetry](https://python-poetry.org/) for managing
Python dependencies. Kindly go to the website and install it first. After
that, please follow the steps below.

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

1. Finally, check data qualty by running:

    ```bash
    poetry run pytest -v
    ```

To clean up the project, run:

```bash
docker-compose down
```
