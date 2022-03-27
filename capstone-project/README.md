# Climate Change with World Bank Country Profile Data Analysis Pipeline

## Project Overview

This projects builds an automated ETL/ELT data data pipeline. It aims to create a star schema optimized for queries on climate change with world bank country profile data analysis. We extract data from S3, process the data using Spark, save them in Parquet back into S3. Spark process is deployed on a cluster using AWS. At the end, we use the data stored in S3, stage them in Redshift, and transform them into a set of dimensional tables for further analysis.

## Datasets

* [Climate Change: Earth Surface Temperature Data](https://www.kaggle.com/datasets/berkeleyearth/climate-change-earth-surface-temperature-data)
* [World Bank Country Profile](https://public.opendatasoft.com/explore/dataset/worldbank-country-profile/table/?disjunctive.country_name&disjunctive.indicator_name&sort=-year)

## Data Modeling

TBD

## Files and What They Do

| Name | Description |
| - | - |
| `mnt/dags/climate_change_with_worldbank_data_pipeline.py` | An Airflow DAG file that runs the ETL data pipeline on climate change and world bank profile data |
| `spark/app/global_temperature_data_processing.py` | A Spark app that reads the global temperature data from CSV, runs ETL, and saves data in Parquet |
| `spark/app/worldbank_data_processing.py` | A Spark app that reads the world bank country profile data from JSON, runs ETL, and saves data in Parquet |
| `README.md` | README file that provides discussion on this project |
| `.env.local` | A environment file that contains the environment variables we want to override in `docker-compose.yaml` and `docker-compose-spark.yaml` |
| `Dockerfile` | A Dockerfile that contains the instruction how to build an Airflow instance with Amazon EMR provider installed |
| `docker-compose.yaml` | A Docker Compose file that runs an Airflow instance with Amazon EMR provider installed used in this project |
| `Dockerfile-spark` | A Dockerfile that contains the instruction how to build an Airflow instance with Apache Spark provider installed |
| `docker-compose-spark.yaml` | A Docker Compose file that runs an Airflow instance with Apache Spark provider installed |

## Instruction on Running the Project

Running Airflow on local machine:

```sh
cp .env.local .env
echo -e "AIRFLOW_UID=$(id -u)" >> .env
mkdir -p mnt/dags mnt/logs mnt/plugins
docker-compose build
docker-compose up
```
