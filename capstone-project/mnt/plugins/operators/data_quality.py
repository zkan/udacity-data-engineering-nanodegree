from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):

    ui_color = "#89DA59"

    @apply_defaults
    def __init__(self, redshift_conn_id="", checks=[], *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.checks = checks

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for each in self.checks:
            records = redshift.get_records(each["test_sql"])
            num_records = records[0][0]
            message = f"Data quality check for test case '{each['test_case']}' failed."
            if each["comparison"] == "=" and num_records != each["expected_result"]:
                raise ValueError(message + " failed.")
            elif each["comparison"] == ">" and num_records <= each["expected_result"]:
                raise ValueError(message + " failed.")
            elif each["comparison"] == "<" and num_records >= each["expected_result"]:
                raise ValueError(message + " failed.")

        self.log.info("All data quality checks passed.")
