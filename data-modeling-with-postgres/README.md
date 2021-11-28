# Data Modeling with Postgres

I define the fact and dimension tables for a star schema for a song play
analysis, and write an ETL pipeline to transform data from files and load
into the tables in Postgres using Python and SQL.

## Song Play Analysis

In this project, we're interested in understanding what songs users
are listening to.

## How to Run this Project

I use [Poetry](https://python-poetry.org/) for managing Python dependencies,
so please install it first.

### Installing Dependencies

```bash
poetry install
```

### Creating Tables

```bash
poetry run python create_tables.py
```