# TBD

## Project Overview

Climate change with world bank country profile 

## Datasets

* [Climate Change: Earth Surface Temperature Data](https://www.kaggle.com/datasets/berkeleyearth/climate-change-earth-surface-temperature-data)
* [World Bank Country Profile](https://public.opendatasoft.com/explore/dataset/worldbank-country-profile/table/?disjunctive.country_name&disjunctive.indicator_name&sort=-year)

## Data Modeling

TBD

## Files and What They Do

| Name | Description |
| - | - |
| `mnt/dags/climate_change_with_worldbank_data_pipeline.py` | An Airflow DAG file that runs the ETL data pipeline on climate change and world bank profile data |
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
