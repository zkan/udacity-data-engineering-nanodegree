from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import (
    EmrAddStepsOperator,
    EmrCreateJobFlowOperator,
    EmrTerminateJobFlowOperator,
)
from airflow.providers.amazon.aws.sensors.emr_step import EmrStepSensor
from airflow.utils import timezone

from operators import (
    StageToRedshiftOperator,
)


AWS_CONN_ID = "aws_default"
BUCKET_NAME = "zkan-capstone-project"

s3_global_temperature_data = "raw/global-temperature/GlobalLandTemperaturesByCity-small.csv"
# s3_global_temperature_data = "raw/global-temperature/GlobalLandTemperaturesByCity.csv"
s3_global_temperature_script = "scripts/global_temperature_data_processing.py"
s3_global_temperature_cleansed = "cleansed/global_temperature/"
hdfs_global_temperature_data = "/global_temperature_data"
hdfs_global_temperature_output = "/global_temperature_output"

s3_worldbank_data = "raw/worldbank/worldbank-country-profile-small.json"
# s3_worldbank_data = "raw/worldbank/worldbank-country-profile.json"
s3_worldbank_script = "scripts/worldbank_data_processing.py"
s3_worldbank_cleansed = "cleansed/worldbank/"
hdfs_worldbank_data = "/worldbank_data"
hdfs_worldbank_output = "/worldbank_output"

JOB_FLOW_OVERRIDES = {
    "Name": "Climate Change with World Bank Country Profile Data Processing",
    "ReleaseLabel": "emr-5.34.0",
    "Applications": [{"Name": "Hadoop"}, {"Name": "Spark"}],
    "Configurations": [
        {
            "Classification": "spark-env",
            "Configurations": [
                {
                    "Classification": "export",
                    "Properties": {"PYSPARK_PYTHON": "/usr/bin/python3"},
                }
            ],
        }
    ],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master node",
                "Market": "SPOT",
                "InstanceRole": "MASTER",
                "InstanceType": "m4.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Core - 2",
                "Market": "SPOT",
                "InstanceRole": "CORE",
                "InstanceType": "m4.xlarge",
                "InstanceCount": 2,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
}

SPARK_STEPS = [
    {
        "Name": "Move raw data from S3 to HDFS",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "s3-dist-cp",
                "--src=s3://{{ params.BUCKET_NAME }}/{{ params.s3_data }}",
                "--dest={{ params.hdfs_data }}",
            ],
        },
    },
    {
        "Name": "Process data",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "client",
                "s3://{{ params.BUCKET_NAME }}/{{ params.s3_script }}",
            ],
        },
    },
    {
        "Name": "Move clean data from HDFS to S3",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "s3-dist-cp",
                "--src={{ params.hdfs_output }}",
                "--dest=s3://{{ params.BUCKET_NAME }}/{{ params.s3_cleansed }}",
            ],
        },
    },
]
LAST_STEP = len(SPARK_STEPS) - 1

default_args = {
    "owner": "Kan Ouivirach",
    "start_date": timezone.datetime(2022, 3, 20),
}
with DAG(
    "climate_change_with_worldbank_data_pipeline",
    default_args=default_args,
    schedule_interval=None,
) as dag:

    create_emr_cluster = EmrCreateJobFlowOperator(
        task_id="create_emr_cluster",
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id=AWS_CONN_ID,
    )

    step_adder_for_global_temperature = EmrAddStepsOperator(
        task_id="add_steps_for_global_temperature",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        aws_conn_id=AWS_CONN_ID,
        steps=SPARK_STEPS,
        params={
            "BUCKET_NAME": BUCKET_NAME,
            "s3_data": s3_global_temperature_data,
            "s3_script": s3_global_temperature_script,
            "s3_cleansed": s3_global_temperature_cleansed,
            "hdfs_data": hdfs_global_temperature_data,
            "hdfs_output": hdfs_global_temperature_output,
        },
    )

    step_adder_for_worldbank = EmrAddStepsOperator(
        task_id="add_steps_for_worldbank",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        aws_conn_id=AWS_CONN_ID,
        steps=SPARK_STEPS,
        params={
            "BUCKET_NAME": BUCKET_NAME,
            "s3_data": s3_worldbank_data,
            "s3_script": s3_worldbank_script,
            "s3_cleansed": s3_worldbank_cleansed,
            "hdfs_data": hdfs_worldbank_data,
            "hdfs_output": hdfs_worldbank_output,
        },
    )

    step_checker_for_global_temperature = EmrStepSensor(
        task_id="watch_step_for_global_temperature",
        job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='add_steps_for_global_temperature', key='return_value')["
        + str(LAST_STEP)
        + "] }}",
        aws_conn_id=AWS_CONN_ID,
    )

    step_checker_for_worldbank = EmrStepSensor(
        task_id="watch_step_worldbank",
        job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='add_steps_for_worldbank', key='return_value')["
        + str(LAST_STEP)
        + "] }}",
        aws_conn_id=AWS_CONN_ID,
    )

    terminate_emr_cluster = EmrTerminateJobFlowOperator(
        task_id="terminate_emr_cluster",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        aws_conn_id=AWS_CONN_ID,
    )

    stage_global_temperature_to_redshift = StageToRedshiftOperator(
        task_id="stage_global_temperature_to_redshift",
        redshift_conn_id="redshift",
        iam_role="arn:aws:iam::573529480358:role/myRedshiftRole",
        table="staging_global_temperature",
        s3_bucket=BUCKET_NAME,
        s3_key=s3_global_temperature_cleansed,
    )

    stage_worldbank_to_redshift = StageToRedshiftOperator(
        task_id="stage_worldbank_to_redshift",
        redshift_conn_id="redshift",
        iam_role="arn:aws:iam::573529480358:role/myRedshiftRole",
        table="staging_worldbank",
        s3_bucket=BUCKET_NAME,
        s3_key=s3_worldbank_cleansed,
    )

    (
        create_emr_cluster
        >> step_adder_for_global_temperature
        >> step_checker_for_global_temperature
        >> terminate_emr_cluster
    )
    (
        create_emr_cluster
        >> step_adder_for_worldbank
        >> step_checker_for_worldbank
        >> terminate_emr_cluster
    )

    terminate_emr_cluster >> [stage_global_temperature_to_redshift, stage_worldbank_to_redshift]
