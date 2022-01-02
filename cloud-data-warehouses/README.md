# Song Play Analysis

## Project Overview

In this project, we create a star schema optimized for queries on songs play analysis.

## Database Schema

## Files and What They Do

| Name | Description |
| - | - |
| `data_modeling_with_cassandra.ipynb` |  A Jupyter notebook file that runs the ETL pipeline for processing the event files and loading to Apache Cassandra tables |
| `Project_1B_ Project_Template.ipynb` |  A Jupyter notebook file that is used as a project template |
| `docker-compose.yaml` | A Docker Compose file that runs a Cassandra cluster used in this project |
| `pyproject.toml` | A file that contains Python package dependencies managed by Poetry for this code repository |

## Instruction on Running the Python Scripts

Before we start running the ETL pipeline, we will need to start a Cassandra cluster
first. Run:

```bash
docker-compose up -d
```

This project uses the [Poetry](https://python-poetry.org/) for managing Python dependencies.
Kindly go to the website and install it first. After that, please follow the steps below.

1. Install the dependencies.

    ```bash
    poetry install
    ```

1. Start the Jupyter server to work on the notebook in this project.

    ```bash
    poetry run jupyter notebook
    ```

To stop the Cassandra cluster, run:

```bash
docker-compose down
```
