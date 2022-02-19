from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):

    ui_color = "#89DA59"

    @apply_defaults
    def __init__(self, redshift_conn_id="", table="", column="", *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.column = column

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        records = redshift.get_records(
            f"SELECT COUNT({self.column}) FROM {self.table} WHERE {self.column} IS NULL"
        )

        num_records = records[0][0]
        if num_records != 0:
            raise ValueError(
                f"Data quality check failed. Column {self.column} in {self.table} has NULL values"
            )

        self.log.info(
            f"Data quality on table {self.table} check passed. No NULL values in the column {self.column}."
        )
