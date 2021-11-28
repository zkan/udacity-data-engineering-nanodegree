# Data Modeling with Postgres

I define the fact and dimension tables for a star schema for a song play
analysis, and write an ETL pipeline to transform data from files and load
into the tables in Postgres using Python and SQL.

## Song Play Analysis

In this project, we're interested in understanding what songs users
are listening to.

## How to Run this Project

Before we start, we'll need to run the Postgres database. In this project,
I set up the Docker Compose for it, so run the following command first.

```bash
docker-compose up
```

For managing Python dependencies, I use [Poetry](https://python-poetry.org/),
so please install it first.

### Installing Dependencies

```bash
poetry install
```

### Creating Tables

```bash
poetry run python create_tables.py
```